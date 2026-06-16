module.exports = {
  branches: ["main"],
  plugins: [
    "@semantic-release/commit-analyzer",
    [
      "@semantic-release/release-notes-generator",
      {
        writerOpts: {
          finalizeContext: (context) => {
            const helmCommits = [];
            const operatorCommits = [];
            const otherCommits = [];

            context.commitGroups.forEach(group => {
              group.commits.forEach(commit => {
                const scope = (commit.scope || '').toLowerCase();
                if (scope === 'helm' || scope === 'chart') {
                  helmCommits.push(commit);
                } else if (scope === 'operator' || scope === 'crd' || scope === 'controller') {
                  operatorCommits.push(commit);
                } else {
                  otherCommits.push(commit);
                }
              });
            });

            const newGroups = [];
            if (operatorCommits.length > 0) {
              newGroups.push({ title: 'Operator / Core Changes', commits: operatorCommits });
            }
            if (helmCommits.length > 0) {
              newGroups.push({ title: 'Helm Chart Changes', commits: helmCommits });
            }
            if (otherCommits.length > 0) {
              newGroups.push({ title: 'Other Changes', commits: otherCommits });
            }

            context.commitGroups = newGroups;
            return context;
          }
        }
      }
    ],
    "@semantic-release/changelog",
    [
      "@semantic-release-cargo/semantic-release-cargo",
      {
        publish: false
      }
    ],
    [
      "semantic-release-helm3",
      {
        chartPath: "charts/surreal-dbops"
      }
    ],
    [
      "@semantic-release/git",
      {
        assets: [
          "Cargo.toml",
          "Cargo.lock",
          "charts/surreal-dbops/Chart.yaml",
          "CHANGELOG.md"
        ],
        message: "chore(release): bump version to ${nextRelease.version} [skip ci]"
      }
    ],
    [
      "@semantic-release/github",
      {
        assets: []
      }
    ]
  ]
};
