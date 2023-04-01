use anyhow;

pub fn init_directory_structure() -> anyhow::Result<()> {

    let curr_dir = std::env::current_dir()?;

    println!("current dir: {:?}",curr_dir);

    // for entry in std::fs::read_dir(curr_dir)? {
    //     let entry = entry?;
    //     let path = entry.path();
    //     println!("Path: {:?}",path);
    //     println!("Metadata: {:?}",entry.metadata()?);
    // }

    std::fs::create_dir_all("./.dbtvc")?;


    return Ok(());
}


