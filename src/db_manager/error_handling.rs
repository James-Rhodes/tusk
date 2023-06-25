use colored::Colorize;
use sqlx::{postgres::PgDatabaseError, Error};

pub fn get_db_error(e: Error) -> String {
    return match e {
        sqlx::Error::Database(e) => match e.try_downcast::<PgDatabaseError>() {
            Ok(e) => {
                let message = e.message();

                let detail = e.detail().unwrap_or_default();
                let hint = e.hint().unwrap_or_default();

                let pos = match e.position() {
                    Some(sqlx::postgres::PgErrorPosition::Original(position)) => {
                        position.to_string()
                    }
                    Some(sqlx::postgres::PgErrorPosition::Internal { position, query }) => {
                        format!("{} for query {}", position.to_string(), query.to_string())
                    }
                    None => String::from(""),
                };
                format!(
                    "{}: {}, Position: {}, Detail: {}, Hint: {}",
                    "Error".red(),
                    message,
                    pos,
                    detail,
                    hint
                )
            }
            Err(e) => format!("{}: {}", "Error".red(), e.to_string()),
        },
        _ => format!("{}: An unexpected error occured", "Error".red()),
    };
}
