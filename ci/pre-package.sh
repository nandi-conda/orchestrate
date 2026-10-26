# Run by nandi-conda/ci's conda-package workflow before it builds recipe/recipe.yaml.
# Every run publishes a new version, so each push to main reaches the channel.
echo "ORCHESTRATE_VERSION=0.1.${GITHUB_RUN_NUMBER:?}" >> "${GITHUB_ENV:?}"
