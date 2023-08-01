use anyhow::{Context, Result};

// This could easily be made a trait in the future so that different doc comment styles can be
// adopted. Only supporting JS doc notation for now
#[derive(Clone, Default, Debug, Eq, PartialEq)]
pub struct FunctionDocParser<'f> {
    pub function_name: &'f str,      // Get this from the file path
    pub function_full_name: &'f str, // Get this from declaration
    pub schema: &'f str,
    pub description: &'f str,
    pub author: Option<&'f str>,                // @author Homer Simpson
    pub date: Option<&'f str>,                  // @date 01/02/1234
    pub example: Option<&'f str>,               // @example SELECT * FROM foobarbaz
    pub params: Option<Vec<FunctionParam<'f>>>, // @param {TEXT} the_name the description
    pub returns: Option<FunctionReturn<'f>>,    // @return {TEXT} the description
}

impl<'f> FunctionDocParser<'f> {
    pub fn new(
        schema: &'f str,
        function_name: &'f str,
        file_contents: &'f str,
    ) -> Result<Option<Self>> {
        
        let doc_comment = match Self::find_doc_comment(file_contents)? {
            Some(dc) => dc,
            None => return Ok(None)
        };

        return Ok(Some(Self {
            function_name,
            function_full_name: Self::get_full_name(function_name, file_contents)?,
            schema,
            description: Self::get_description(doc_comment)?,
            author: Self::get_single_doc_tag(doc_comment, "@author"),
            date: Self::get_single_doc_tag(doc_comment, "@date"),
            example: Self::get_single_doc_tag(doc_comment, "@example"),
            params: Self::get_params(doc_comment)?,
            returns: Self::get_return(doc_comment)?,
        }));
    }

    pub fn find_doc_comment(file_contents: &'f str) -> Result<Option<&'f str>> {
        let doc_start_pos = match file_contents
            .find("/**") {
                Some(pos) => pos + 3,
                None => return Ok(None)
            };

        let doc_end_pos = file_contents[doc_start_pos..]
            .find("*/")
            .context("Invalid doc comments for function")?
            + doc_start_pos; // 2 so that the string includes */ at the end

        Ok(Some(file_contents[doc_start_pos..doc_end_pos].trim()))
    }

    pub fn get_full_name(function_name: &'f str, file_contents: &'f str) -> Result<&'f str> {
        let declaration_start = file_contents
            .find(function_name)
            .context("The function name could not be found in the function declaration")?;
        let declaration_end = declaration_start
            + file_contents[declaration_start..]
                .find(")")
                .context("There was not a closing bracket found within the function")?
            + 1; // + 1 to include the bracket

        Ok(&file_contents[declaration_start..declaration_end])
    }

    pub fn get_description(doc_comment: &'f str) -> Result<&'f str> {
        let description_end = doc_comment.find("@").unwrap_or_else(|| doc_comment.len());

        return Ok(doc_comment[..description_end].trim());
    }

    pub fn get_single_doc_tag(doc_comment: &'f str, element_name: &str) -> Option<&'f str> {
        let start_element = doc_comment.find(element_name);
        if start_element.is_none() {
            return None;
        }
        let start_element = start_element.unwrap() + element_name.len();

        let end_element = match doc_comment[start_element..].find("@") {
            Some(loc) => start_element + loc,
            None => doc_comment.len(),
        };

        Some(doc_comment[start_element..end_element].trim())
    }

    pub fn get_params(doc_comment: &'f str) -> Result<Option<Vec<FunctionParam>>> {
        let mut start_param = doc_comment.find("@param");

        if start_param.is_none() {
            // There is no return statement
            return Ok(None);
        }

        let mut end_param_loc = 0;
        let mut all_params = vec![];
        while let Some(start_param_loc) = start_param {
            let start_param_loc = start_param_loc + end_param_loc;
            let end_param = doc_comment[start_param_loc + 1..].find("@");
            if end_param.is_none() {
                all_params.push(FunctionParam::new(doc_comment[start_param_loc..].trim())?);
                break;
            }
            end_param_loc = start_param_loc + end_param.unwrap();

            all_params.push(FunctionParam::new(
                doc_comment[start_param_loc..end_param_loc].trim(),
            )?);

            start_param = doc_comment[end_param_loc..].find("@param");
        }

        Ok(Some(all_params))
    }

    pub fn get_return(doc_comment: &'f str) -> Result<Option<FunctionReturn>> {
        let start_return = doc_comment.find("@return");

        if start_return.is_none() {
            // There is no return statement
            return Ok(None);
        }

        let start_return = start_return.unwrap();

        Ok(Some(FunctionReturn::new(
            doc_comment[start_return..doc_comment.len()].trim(),
        )?))
    }
}

#[derive(Clone, Default, Debug, Eq, PartialEq)]
pub struct FunctionParam<'p> {
    pub name: &'p str,
    pub param_type: &'p str,
    pub description: Option<&'p str>,
}
impl<'p> FunctionParam<'p> {
    fn new(param_string: &'p str) -> Result<Self> {
        let start_type_declaration = param_string
            .find("{")
            .context("There was no type declaration for the given param statement")?
            + 1;
        let end_type_declaration = param_string
            .find("}")
            .context("Badly formatted type declaration for param statement")?;

        let param_type = param_string[start_type_declaration..end_type_declaration].trim();

        let mut start_name = end_type_declaration + 1;
        let mut end_name = end_type_declaration + 1;

        let the_rest = &param_string[end_type_declaration + 1..];
        let mut has_seen_char = false;

        for (idx, c) in the_rest.chars().enumerate() {
            match (c.is_whitespace(), has_seen_char) {
                (true, true) => {
                    end_name += idx;
                    break;
                }
                (true, false) | (false, true) => continue,
                (false, false) => {
                    has_seen_char = true;
                    start_name += idx;
                }
            }
        }

        let name = &param_string[start_name..end_name];

        let description =
            if end_name != param_string.len() && param_string[end_name + 1..].trim() != "" {
                Some(param_string[end_name + 1..].trim())
            } else {
                None
            };

        Ok(Self {
            name,
            param_type,
            description,
        })
    }
}

#[derive(Clone, Default, Debug, Eq, PartialEq)]
pub struct FunctionReturn<'r> {
    pub return_type: &'r str,
    pub description: Option<&'r str>,
}

impl<'r> FunctionReturn<'r> {
    fn new(return_string: &'r str) -> Result<Self> {
        let start_type_declaration = return_string
            .find("{")
            .context("There was no type declaration for the given return statement")?
            + 1;
        let end_type_declaration = return_string
            .find("}")
            .context("Badly formatted type declaration for return statement")?;

        let return_type = return_string[start_type_declaration..end_type_declaration].trim();

        let description = return_string[end_type_declaration + 1..].trim();
        let description = if description == "" {
            None
        } else {
            Some(description)
        };

        Ok(Self {
            return_type,
            description,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn get_doc_string_works() {
        let input = r#"
SOME STUFF THAT WONT BE IN THE RESULT

/**
   THIS WILL BE IN THE RESULT
 
 */
        "#;

        let expected_result = r#"THIS WILL BE IN THE RESULT"#;

        assert_eq!(
            expected_result,
            FunctionDocParser::find_doc_comment(input).unwrap().unwrap()
        );
    }

    #[test]
    fn get_full_name_works() {
        let input = r#"
THIS IS NOT SUPPOSED TO GET ACCOUNTED FOR
CREATE OR REPLACE FUNCTION my_schema.my_test_func(SOME INPUT STUFF)



THIS IS JUST HERE FOR A BUFFER
"#;

        assert_eq!(
            "my_test_func(SOME INPUT STUFF)",
            FunctionDocParser::get_full_name("my_test_func", input).unwrap()
        );
    }

    #[test]
    fn get_description_works() {
        let input = r#"This is the function description

		@param {TEXT} var1 The first input param
		@param {TEXT} var2 The second input param

		@return {TEXT} this is a return description"#;

        assert_eq!(
            "This is the function description",
            FunctionDocParser::get_description(input).unwrap()
        );

        let input = r#"
		This is the function description

	"#;

        assert_eq!(
            "This is the function description",
            FunctionDocParser::get_description(input).unwrap()
        );
    }

    #[test]
    fn get_return_info_works() {
        let input = r#"This is the function description

		@param {TEXT} var1 The first input param
		@param {TEXT} var2 The second input param

		@return {TEXT} this is a return description"#;

        assert_eq!(
            Some(FunctionReturn {
                return_type: "TEXT",
                description: Some("this is a return description")
            }),
            FunctionDocParser::get_return(input).unwrap()
        );

        let input = r#"This is the function description

		@param {TEXT} var1 The first input param
		@param {TEXT} var2 The second input param

		@return {TEXT}"#;

        assert_eq!(
            Some(FunctionReturn {
                return_type: "TEXT",
                description: None
            }),
            FunctionDocParser::get_return(input).unwrap()
        );

        let input = r#"This is the function description

		@param {TEXT} var1 The first input param
		@param {TEXT} var2 The second input param"#;

        assert_eq!(None, FunctionDocParser::get_return(input).unwrap());

        let input = r#"This is the function description

		@param {TEXT} var1 The first input param
		@param {TEXT} var2 The second input param

		@return {TEXT this is a return description"#;

        assert_eq!(true, FunctionDocParser::get_return(input).is_err());

        let input = r#"This is the function description

		@param {TEXT} var1 The first input param
		@param {TEXT} var2 The second input param

		@return TEXT this is a return description"#;

        assert_eq!(true, FunctionDocParser::get_return(input).is_err());

        let input = r#"This is the function description

		@param {TEXT} var1 The first input param
		@param {TEXT} var2 The second input param

		@return TEXT} this is a return description"#;

        assert_eq!(true, FunctionDocParser::get_return(input).is_err());
    }

    #[test]
    fn get_params_works() {
        let input = r#"This is the function description

		@param {TEXT} var1 The first input param
		@param {TEXT} var2 The second input param

		@return {TEXT} this is a return description"#;

        assert_eq!(
            Some(vec![
                FunctionParam {
                    name: "var1",
                    param_type: "TEXT",
                    description: Some("The first input param")
                },
                FunctionParam {
                    name: "var2",
                    param_type: "TEXT",
                    description: Some("The second input param")
                }
            ]),
            FunctionDocParser::get_params(input).unwrap()
        );

        let input = r#"This is the function description

		@param {TEXT} var1 The first input param
		@param TEXT} var2 The second input param

		@return {TEXT} this is a return description"#;

        assert_eq!(true, FunctionDocParser::get_params(input).is_err());

        let input = r#"This is the function description

		@param {TEXT} var1 The first input param
		@param {TEXT}

		@return {TEXT} this is a return description"#;

        assert_eq!(
            Some(vec![
                FunctionParam {
                    name: "var1",
                    param_type: "TEXT",
                    description: Some("The first input param")
                },
                FunctionParam {
                    name: "",
                    param_type: "TEXT",
                    description: None
                }
            ]),
            FunctionDocParser::get_params(input).unwrap()
        );
    }

    #[test]
    fn get_single_doc_element_works() {
        let input = r#"This is the function description

        @author Homer Simpson
		@param {TEXT} var1 The first input param
		@param {TEXT} var2 The second input param

		@return {TEXT} this is a return description"#;

        assert_eq!(
            "Homer Simpson",
            FunctionDocParser::get_single_doc_tag(input, "@author").unwrap()
        );

        let input = r#"This is the function description

		@param {TEXT} var1 The first input param
		@param {TEXT} var2 The second input param

		@return {TEXT} this is a return description
        @author Homer Simpson"#;

        assert_eq!(
            "Homer Simpson",
            FunctionDocParser::get_single_doc_tag(input, "@author").unwrap()
        );
    }

    #[test]
    fn func_doc_info_new_works() {
        let input = r#"
    CREATE OR REPLACE FUNCTION public.concatenating(var1 text, var2 text)
     RETURNS text
     LANGUAGE plpgsql
    AS $function$
    	BEGIN
    	/**
    		This is the function description

            @author Homer Simpson
            @date 01/02/1234
            @example SELECT public.concatenating('Hello ', 'World');

    		@param {TEXT} var1 The first part of the output
    		@param {TEXT} var2 The second part of the output

    		@return {TEXT} var1 and var2 concatenated together
    	*/
    	INSERT INTO public.test_three(id, something_woohoo) VALUES (987654321, 123.123456); -- Side effect woohoo
    		RAISE NOTICE 'IZZOS';
    		return var1 || var2;
    	END
    $function$"#;

        let expected_result = FunctionDocParser {
            function_name: "concatenating",
            function_full_name: "concatenating(var1 text, var2 text)",
            schema: "public",
            description: "This is the function description",
            author: Some("Homer Simpson"),
            date: Some("01/02/1234"),
            example: Some("SELECT public.concatenating('Hello ', 'World');"),
            params: Some(vec![
                FunctionParam {
                    name: "var1",
                    param_type: "TEXT",
                    description: Some("The first part of the output"),
                },
                FunctionParam {
                    name: "var2",
                    param_type: "TEXT",
                    description: Some("The second part of the output"),
                },
            ]),
            returns: Some(FunctionReturn {
                return_type: "TEXT",
                description: Some("var1 and var2 concatenated together"),
            }),
        };

        assert_eq!(
            expected_result,
            FunctionDocParser::new("public", "concatenating", input).unwrap().unwrap()
        );
    }
}
