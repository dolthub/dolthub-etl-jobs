
def get_commit_message(git_hash: str, github_actions_run_url: str) -> str:
    return f"""Generated by GitHub Actions from commit {git_hash}, see run at {github_actions_run_url}"""
