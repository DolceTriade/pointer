use leptos::either::EitherOf3;
use leptos::prelude::*;
use leptos_router::hooks::{use_navigate, use_params};
use leptos_router::params::Params;

#[derive(Params, Debug, PartialEq)]
struct RepoParams {
    repo: String,
}

#[server]
pub async fn get_repo_branches(repo: String) -> Result<Vec<String>, ServerFnError> {
    use crate::db::{Database, postgres::PostgresDb};

    let state = expect_context::<crate::server::GlobalAppState>();
    let state = state.lock().await;
    let db = PostgresDb::new(state.pool.clone());

    let branches = db
        .get_branches_for_repository(&repo)
        .await
        .map_err(|e| ServerFnError::new(e.to_string()))?;

    Ok(branches)
}

#[component]
pub fn RepoDetailPage() -> impl IntoView {
    let params = use_params::<RepoParams>();
    let repo_name = move || {
        params.with(|p| match p {
            Ok(params) => params.repo.clone(),
            Err(_) => "Unknown Repository".to_string(),
        })
    };

    let branches = Resource::new(repo_name, |repo| get_repo_branches(repo));

    Effect::new(move |_| {
        if let Some(Ok(branches)) = branches.get() {
            if let Some(latest_branch) = branches.first() {
                let navigate = use_navigate();
                let to = format!("/repo/{}/tree/{}", repo_name(), latest_branch);
                navigate(&to, Default::default());
            }
        }
    });

    view! {
        <main class="flex-grow flex flex-col items-center justify-start pt-8 p-4">
            <Suspense fallback=move || {
                view! { <p>"Loading repository..."</p> }
            }>
                {move || {
                    branches
                        .get()
                        .map(|res| match res {
                            Err(e) => {
                                return EitherOf3::A(
                                    view! {
                                        <p class="text-red-500">
                                            "Error loading repository: " {e.to_string()}
                                        </p>
                                    },
                                );
                            }
                            Ok(branches) if branches.is_empty() => {
                                return EitherOf3::B(
                                    view! { <p>"This repository has no indexed branches."</p> },
                                );
                            }
                            _ => {
                                return EitherOf3::C(
                                    view! { <p>"Redirecting to file viewer..."</p> },
                                );
                            }
                        })
                }}
            </Suspense>
        </main>
    }
}
