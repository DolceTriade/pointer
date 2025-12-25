{
  config,
  lib,
  pkgs,
  ...
}:
with lib; let
  cfg = config.services.pointer;

  # Helper function to create environment variables for a service component
  mkEnv = {
    database_url,
    bind,
    max_connections,
    gc ? null,
    gc_interval_secs ? null,
    debug ? false,
  }:
    {RUST_LOG = "info";}
    // (optionalAttrs (debug == true) {POINTER_EXPLAIN_SEARCH_SQL = toString 1;})
    // (optionalAttrs (database_url != null) {DATABASE_URL = database_url;})
    // (optionalAttrs (bind != null) {BIND_ADDRESS = bind;})
    // (optionalAttrs (max_connections != null) {MAX_CONNECTIONS = toString max_connections;})
    // (optionalAttrs (gc != null) {
      ENABLE_GC =
        if gc
        then "true"
        else "false";
    })
    // (optionalAttrs (gc_interval_secs != null) {GC_INTERVAL_SECS = toString gc_interval_secs;});
in {
  options.services.pointer = {
    enable = mkEnableOption "the Pointer service (frontend and backend).";

    package = mkOption {
      type = types.package;
      default = pkgs.pointer; # Assuming a package named 'pointer' exists in your nixpkgs
      defaultText = literalExpression "pkgs.pointer";
      description = "The Pointer package containing both 'pointer' and 'pointer-backend' binaries.";
    };

    # --- Pointer (Frontend) Configuration ---
    pointer = {
      enable = mkEnableOption "the Pointer frontend component.";

      database_url = mkOption {
        type = types.str;
        description = "Sets the DATABASE_URL environment variable for the 'pointer' service.";
      };

      bind = mkOption {
        type = types.str;
        default = "127.0.0.1:3000";
        description = "Sets the BIND_ADDRESS environment variable for the 'pointer' service.";
      };

      max_connections = mkOption {
        type = types.int;
        default = 10;
        description = "Sets the MAX_CONNECTIONS environment variable for the 'pointer' service.";
      };

      debug = mkOption {
        type = types.bool;
        default = false;
        description = "Enable debug logging for SQL queries.";
      };
    };

    # --- Pointer-Backend Configuration ---
    pointerBackend = {
      enable = mkEnableOption "the Pointer backend component.";

      database_url = mkOption {
        type = types.str;
        description = "Sets the DATABASE_URL environment variable for the 'pointer-backend' service.";
      };

      bind = mkOption {
        type = types.str;
        default = "127.0.0.1:8080";
        description = "Sets the BIND_ADDRESS environment variable for the 'pointer-backend' service.";
      };

      max_connections = mkOption {
        type = types.int;
        default = 10;
        description = "Sets the MAX_CONNECTIONS environment variable for the 'pointer-backend' service.";
      };

      gc = mkOption {
        type = types.bool;
        default = true;
        description = "Sets the ENABLE_GC environment variable for the 'pointer-backend' service.";
      };

      gc_interval_secs = mkOption {
        type = types.int;
        default = 3600 * 24;
        description = "Sets the GC_INTERVAL_SECS environment variable for the 'pointer-backend' service.";
      };
    };
  };

  config = mkIf cfg.enable {
    # --- Systemd Service for Pointer (Frontend) ---
    systemd.services.pointer-frontend = mkIf cfg.pointer.enable {
      description = "Pointer Frontend Service";
      wantedBy = ["multi-user.target"];
      after = ["network.target"]; # Add database service here if needed
      environment = mkEnv {
        database_url = cfg.pointer.database_url;
        bind = cfg.pointer.bind;
        max_connections = cfg.pointer.max_connections;
        debug = cfg.pointer.debug;
      };
      serviceConfig = {
        ExecStart = "${cfg.package}/bin/pointer";
        User = "pointer"; # Recommended to run as a non-root user
        Group = "pointer";
        Restart = "always";
      };
    };

    # --- Systemd Service for Pointer-Backend ---
    systemd.services.pointer-backend = mkIf cfg.pointerBackend.enable {
      description = "Pointer Backend Service";
      wantedBy = ["multi-user.target"];
      after = ["network.target"]; # Add database service here if needed
      environment = mkEnv {
        database_url = cfg.pointerBackend.database_url;
        bind = cfg.pointerBackend.bind;
        max_connections = cfg.pointerBackend.max_connections;
        gc = cfg.pointerBackend.gc;
        gc_interval_secs = cfg.pointerBackend.gc_interval_secs;
      };
      serviceConfig = {
        ExecStart = "${cfg.package}/bin/pointer-backend";
        User = "pointer";
        Group = "pointer";
        Restart = "always";
        WorkingDirectory = "/home/pointer";
      };
    };

    users.groups.pointer = {};
    users.users.pointer = {
      createHome = true;
      description = "Pointer";
      extraGroups = [];
      group = "pointer";
      home = "/home/pointer";
      shell = "/bin/sh";
      isNormalUser = true;
    };
  };
}
