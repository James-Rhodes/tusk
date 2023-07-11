use std::collections::HashSet;

use anyhow::Result;

pub struct ChangeStatus {
    pub added: u32,
    pub removed: u32,
    pub amount_before_change: u32,
}

pub fn format_config_file(file_path: &str) -> Result<()> {
    let local_file_contents = std::fs::read_to_string(file_path)
        .unwrap_or_else(|_| panic!("The config file found at {} should exist", file_path));

    std::fs::write(
        file_path,
        format_config_file_contents(&local_file_contents, &None).join("\n"),
    )?;

    Ok(())
}

fn format_config_file_contents(
    file_contents: &str,
    to_filter_out: &Option<&HashSet<&String>>,
) -> Vec<String> {
    let mut file_contents: Vec<String> = file_contents
        .trim()
        .lines()
        .filter_map(|line| {
            let db_item = (*line).trim().replace(' ', "");

            // Filter out empty lines
            if db_item.is_empty() {
                return None;
            }

            if let Some(to_filter_out) = to_filter_out {
                if to_filter_out.contains(&(db_item.replace("//", "").replace('#', ""))) {
                    return None;
                }
            }

            Some(db_item)
        })
        .collect();

    file_contents.sort_by_key(|val| val.replace("//", "").replace([' ', '#'], ""));

    file_contents
}

pub fn get_uncommented_file_contents(file_path: &str) -> Result<Vec<String>> {
    let result = std::fs::read_to_string(file_path)?
        .lines()
        .filter_map(|line| {
            let line_trimmed = line.trim();
            if line_trimmed.starts_with("//") || line_trimmed.starts_with('#') {
                return None;
            }
            Some(String::from(line))
        })
        .collect::<Vec<String>>();

    Ok(result)
}

pub fn get_commented_file_contents(file_path: &str) -> Result<Vec<String>> {
    let result = std::fs::read_to_string(file_path)?
        .lines()
        .filter_map(|line| {
            let line = line.trim();
            if line.starts_with("//") || line.starts_with('#') {
                return Some(line.replace("//", "").replace('#', ""));
            }
            None
        })
        .collect::<Vec<String>>();

    Ok(result)
}

pub fn get_matching_file_contents<T, U, V>(
    file_contents: T,
    patterns: &[V],
    schema_to_match: Option<&str>,
) -> Result<Vec<U>>
where
    T: Iterator<Item = U>,
    U: AsRef<str>,
    V: AsRef<str>
{
    Ok(file_contents
        .filter(|item| {
            let item = item.as_ref();
            patterns.iter().any(|pat| {
                // item.as_ref().starts_with(pat.as_ref());
                let pat = pat.as_ref();
                match (pat.split_once('.'), schema_to_match) {
                    (Some((schema_name, item_to_match)), Some(schema_to_match)) => {
                        schema_name == schema_to_match
                            && (item.starts_with(item_to_match) || item_to_match == "%")
                    }
                    (None, Some(_)) | (None, None) => item.starts_with(pat),
                    _ => false,
                }
            })
        })
        .collect())
}

pub fn update_file_contents_from_db(
    file_path: &str,
    from_db: HashSet<String>,
    add_new_as_commented: bool,
    delete_items_from_config: bool,
) -> Result<ChangeStatus> {
    // I am certain this function can be made more efficient. I am just running with this as a first draft
    // Benchmarks should be done at some stage to see if this can be improved by simply merging two
    // sorted vecs

    let mut local_file_contents = std::fs::read_to_string(file_path)
        .unwrap_or_else(|_| panic!("The config file found at {} should exist", file_path));

    let all_local_contents: HashSet<String> = local_file_contents
        .lines()
        .map(|line| line.replace("//", "").replace([' ', '#'], ""))
        .collect();

    let not_in_local = from_db.difference(&all_local_contents);
    let not_in_db: HashSet<&String> = all_local_contents.difference(&from_db).collect();

    let mut added = 0;
    not_in_local.into_iter().for_each(|item| {
        if add_new_as_commented {
            local_file_contents.push_str(&(String::from("\n//") + item));
        } else {
            local_file_contents.push_str(&(String::from("\n") + item));
        }
        added += 1;
    });

    if delete_items_from_config {
        std::fs::write(
            file_path,
            format_config_file_contents(&local_file_contents, &Some(&not_in_db)).join("\n"),
        )?;

        return Ok(ChangeStatus {
            added,
            removed: not_in_db.len() as u32,
            amount_before_change: all_local_contents.len() as u32,
        });
    }
    std::fs::write(
        file_path,
        format_config_file_contents(&local_file_contents, &None).join("\n"),
    )?;
    Ok(ChangeStatus {
        added,
        removed: 0,
        amount_before_change: all_local_contents.len() as u32,
    })
}

#[cfg(test)]
mod tests {

    use super::*;

    mod format_config_file_contents_tests {

        use super::*;

        #[test]
        fn format_config_file_contents_works() {
            let file_contents = "A\nB\nC_something with spaces\n//D_commented_out\n//E commented out with spaces   \nto_be_filtered_out\n//to_be_filtered_out_commented   \n#F_HashComment";

            let filter_one = String::from("to_be_filtered_out");
            let filter_two = String::from("to_be_filtered_out_commented");
            let mut filter_list: HashSet<&String> = HashSet::new();

            filter_list.insert(&filter_one);
            filter_list.insert(&filter_two);

            let result = format_config_file_contents(file_contents, &Some(&filter_list));

            assert_eq!(
                result,
                vec![
                    "A",
                    "B",
                    "C_somethingwithspaces",
                    "//D_commented_out",
                    "//Ecommentedoutwithspaces",
                    "#F_HashComment"
                ]
            )
        }
    }

    mod getting_file_contents {
        use super::*;
        use tempfile::tempdir_in;

        #[test]
        fn get_uncommented_file_contents_works() {
            let temp_test_dir =
                tempdir_in(".").expect("Temporary Directory should not fail to be created");
            let file_path = String::from(
                temp_test_dir
                    .path()
                    .join("test_config.txt")
                    .to_str()
                    .unwrap(),
            );
            println!("{}", file_path);

            std::fs::write(&file_path, "//dont_show\nshould_show\nshould show too with spaces\n   //shouldnt show with spaces\n#shouldn't show either with hash").unwrap();

            assert_eq!(
                get_uncommented_file_contents(&file_path)
                    .expect("This should never fail in this scenario"),
                vec!["should_show", "should show too with spaces"]
            );
        }

        #[test]
        fn get_commented_file_contents_works() {
            let temp_test_dir =
                tempdir_in(".").expect("Temporary Directory should not fail to be created");
            let file_path = String::from(
                temp_test_dir
                    .path()
                    .join("test_config.txt")
                    .to_str()
                    .unwrap(),
            );
            println!("{}", file_path);

            std::fs::write(&file_path, "//should_show\nshould_not_show\nshould not show too with spaces\n   //should show with spaces\n#should show with hash").unwrap();

            assert_eq!(
                get_commented_file_contents(&file_path)
                    .expect("This should never fail in this scenario"),
                vec![
                    "should_show",
                    "should show with spaces",
                    "should show with hash"
                ]
            );
        }

        #[test]
        fn get_matching_file_contents_works() {
            let test_uncommented_contents = vec![
                String::from("Test_One"),
                String::from("Test_Two"),
                String::from("unrelated"),
            ];

            assert_eq!(
                get_matching_file_contents(
                    test_uncommented_contents.iter(),
                    &vec![String::from("Test")],
                    None
                )
                .expect("This should never fail in this scenario"),
                vec!["Test_One", "Test_Two"]
            );

            assert_eq!(
                get_matching_file_contents(
                    test_uncommented_contents.iter(),
                    &vec![String::from("Test"), String::from("un")],
                    None
                )
                .expect("This should never fail in this scenario"),
                vec!["Test_One", "Test_Two", "unrelated"]
            );

            assert_eq!(
                get_matching_file_contents(
                    test_uncommented_contents.iter(),
                    &vec![
                        String::from("Test_O"),
                        String::from("schema_name.Test_T"),
                        String::from("not_match.un")
                    ],
                    Some("schema_name")
                )
                .expect("This should never fail in this scenario"),
                vec!["Test_One", "Test_Two"]
            );

            assert_eq!(
                get_matching_file_contents(
                    test_uncommented_contents.iter(),
                    &vec![String::from("schema_name.%"),],
                    Some("schema_name")
                )
                .expect("This should never fail in this scenario"),
                vec!["Test_One", "Test_Two", "unrelated"]
            );
        }
    }
}
