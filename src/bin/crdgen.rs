use kube::CustomResourceExt;
use std::fs::File;
use std::io::Write;
use std::path::Path;
use surreal_dbops::crd::{Database, Instance, Namespace, Rollout, Schema};

fn main() -> anyhow::Result<()> {
    let crds = vec![
        ("Instance.yaml", Instance::crd()),
        ("Namespace.yaml", Namespace::crd()),
        ("Database.yaml", Database::crd()),
        ("Schema.yaml", Schema::crd()),
        ("Rollout.yaml", Rollout::crd()),
    ];

    let crds_dir = Path::new("charts/surreal-dbops/crds");
    if !crds_dir.exists() {
        std::fs::create_dir_all(crds_dir)?;
    }

    for (filename, crd) in crds {
        let path = crds_dir.join(filename);
        let yaml = serde_yaml::to_string(&crd)?;
        let mut file = File::create(&path)?;
        // Ensure exactly one leading --- and exactly one trailing newline
        write!(file, "---\n{}\n", yaml.trim_end())?;
        println!("Generated {}", path.display());
    }

    Ok(())
}
