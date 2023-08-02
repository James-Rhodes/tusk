# Tusk

## What is Tusk?
Tusk is a CLI intended to aid in the development, version control and testing of PL/pgSQL functions.

PL/pgSQL functions often are developed on the database making version control and development tricky. Tusk helps with this by giving a framework for version controlling not only the functions but also the state of the DB while the functions are in development. So rather than just pulling the function DDL, Tusk also can pull down the DDL (using SQL queries or pg_dump) for views, tables, data types, table data and also the functions themselves. Tusk provides methods for getting a list of all items in the data base (tables, views, data types and functions), choosing what you want to put under version control, pulling down the DDL for your selections and then save them using your favourite version control software (primarily git). Tusk also has the functionality to also push functions up to a database meaning development can be done in your favourite development environment under complete version control software and simply push the changes to the functions up to the database. 

Tusk provides unit testing functionality for PL/pgSQL functions all within transactions that are rolled back at the completion of the unit tests meaning any insert, delete or update statements are not saved in the DB. The unit tests can be run on the push of functions to the database and will be rolled back (reversing any changes to the functions) in the event that a unit test fails. 

Tusk also provides a method of generating documentation for PL/pgSQL functions following a standard similar (but not as extensive) as JSDoc. The documentation is compiled from code comments into markdown files.

## What is Tusk Not?
Tusk is not a drop in replacement for general database back up tools such as pg_dump. Tusk is not intended to be used to back up large amounts of data from the database but rather used to store the state of the database when functions were developed. So if something breaks it is easy to see why by also observing the state of the tables, data types etc. Table data can be stored but this is only intended to save small tables that might consist of config items or other information that might be vital to the running of the database functions.

## Usage
For all of the following examples it is assumed that Tusk is either in your path (so you can call it directly with tusk) or in the directory you are intending to back up (so you can exexecute it with ./tusk)

### Initialising the Repo
To initialise a directory for tusk first ensure you are in the directory you wish to store the database items in. Then call the following:

```bash
tusk init # or ./tusk init
```

Running init performs a number of tasks such as:
1. Creates a .tusk directory.
2. Creates the necessary config files within .tusk filling with default values.
3. Creates schemas directory which will store all of the functions and DDL.

After running init it is important that you fill in the necessary values within the environment file found in ./.tusk/.env

**IMPORTANT NOTE:** The .env file should be immediately added to your .gitignore file and should not be under ANY kind of source control.

This file allows you to configure how you would like to connect to the database whether it be through SSH or not. The items that must be populated are shown below: 
| Field Name          | Description |
| ------------------  | ----------- | 
| DB_USER             | The username on the DB you are signing in to.
| DB_PASSWORD         | The password for the above user.
| DB_HOST             | The address of the DB. This automatically gets set to localhost if connecting via ssh.
| DB_PORT             | The port the DB is exposing for connections.
| DB_NAME             | The name of the DB you are connecting to.
| USE_SSH             | Boolean values. Set to TRUE or FALSE to indicate if you would like to connect via SSH. 
| SSH_HOST            | The address of the host you want to SSH in to.
| SSH_USER            | The user you will be port forwarding with during the SSH connection.
| SSH_LOCAL_BIND_PORT | The port on your local machine that will be forwarded to the remote machine.
| PG_BIN_PATH         | The path to the pg_dump binary. If this is left blank then it is assumed that pg_dump is already on your path and can be executed via pg_dump ...

The command that Tusk uses to begin an SSH connection is as follows:

```bash
ssh -M -S backup-socket -fNT -L SSH_LOCAL_BIND_PORT:DB_HOST:DB_PORT SSH_USER@SSH_HOST
```

The socket backup-socket is started so that the connection can be closed at the end of Tusk running using the command:

```bash
ssh -q -S backup-socket -O exit SSH_USER@SSH_HOST
```

After port forwarding tools like pg_dump and the connection Tusk makes to the database are all instead made through your machines SSH_LOCAL_BIND_PORT and so the DB_HOST variable is overwritten internally (after the ssh connection is made to be 127.0.0.1).

There is another additional file that Tusk creates on init that contains user config based on how you want each command to be executed. These are well commented and should be fairly straight forward to alter yourself in the ./.tusk/user_config.yaml file. The defaults are sensible so mostly this can be left untouched unless you want a different behaviour.


### Fetch list of items from the DB
Assuming the project structure has been initialised correctly and the DB can be connected to with the variables in the .env file, you are now ready to run fetch.

Fetch should be the first command you run. On its first run fetch will populate a config file ./.tusk/config/schemas_to_include.conf with all of the schemas that exist on the database. Run: 

```bash
tusk fetch
```

Once this is complete you should go into ./.tusk/config/schemas_to_include.conf and uncomment (Tusk supports // or # as comments) the schemas you wish to place under version control.

Once you have uncommented the schemas you want to place under version control, run fetch again.
```bash
tusk fetch
```

Now that Tusk knows which schemas to back up, it will create additional config files such as:

./.tusk/config/schemas/YOUR_SCHEMA_NAME/table_ddl_to_include.conf
./.tusk/config/schemas/YOUR_SCHEMA_NAME/table_data_to_include.conf
./.tusk/config/schemas/YOUR_SCHEMA_NAME/views_to_include.conf
./.tusk/config/schemas/YOUR_SCHEMA_NAME/data_types_to_include.conf
./.tusk/config/schemas/YOUR_SCHEMA_NAME/functions_to_include.conf

These config files will contain lists of all of the tables, views, data types and functions found in YOUR_SCHEMA_NAME. 

Any uncommented item in any of these config files should be items you wish to place under version control. It is for this reason that by default table_data_to_include.conf has all of its items commented out, because table data should only be backed up for small config tables, or anything pertinent to the correct opperation of any of the functions.

Fetch should be run periodically and will continue to add new items to these config files as the database changes. It will always respect the commented out items but will also sort them in alphabetical order for ease of use. When an item is removed from the DB running fetch will remove the item from the relevent config files. If you would like to change this behaviour and instead leave the item in the config file you can edit the user_config.yaml.

Additionally you can change the default behaviour of new items that get added via fetch to either be commented or uncommented depending on the type of item it is (data type, functions etc.) in the user_config.yaml file.


### Pull DDL from the Database
To pull every single item from the database you can run 
```bash
tusk pull -a
```

If you wish to pull only specific types of items you can run any of the following:
```bash
tusk pull -f # Pulls all functions uncommented in the config files from every schema

tusk pull -d # Pulls all data types uncommented in the config files from every schema

tusk pull -v # Pulls all views uncommented in the config files from every schema

tusk pull -t # Pulls all table DDL uncommented in the config files from every schema

tusk pull -T # Pulls all table Data uncommented in the config files from every schema
```

The items you wish to pull can also be filtered. For example if you wish to pull all functions whose name starts with the word testing, you can run:
```bash
tusk pull -f testing
```

If you wish to pull all table DDL that are uncommented and belong in the schema public then you can run the following:
```bash
tusk pull -t public.% # Gets the DDL of every table in the public schema
```

Additionally if you wish to pull all functions that belong in the public schema AND whose name starts with the word testing then you can run the following:
```bash
tusk pull -f public.testing # Gets the function definitions of all functions in public that start with testing
```

This filtering can be applied to all of the requested DDL types (data types, views, table DDL, table data and functions).


It is important to note (depending on how you have configured your user_config.yaml) that running any of the following: 
```bash
tusk pull -a # Note there are no filtering args

tusk pull -f # Note there are no filtering args

tusk pull -v # Note there are no filtering args

tusk pull -d # Note there are no filtering args

tusk pull -t # Note there are no filtering args

tusk pull -T # Note there are no filtering args
```


Will completely delete the directories corresponding to the command before replacing them with the new data from the database. This enables clearing of old tables etc that no longer exist on the DB because they will be deleted and then when the pull begins will not be repopulated. The only exception to this is in the case of functions with unit tests. Any functions with unit tests will never be cleared. This is to stop any unit tests from accidentally being deleted. To remove functions that no longer exist on the DB you must first remove the unit_tests directory from the functions sub directory.

### Push Function Changes to the DB

Tusk is designed to aid in the development of PL/pgSQL functions and as such also provides the ability to push local changes to functions up to the database. It is important to note that Tusk does not provide the capability to push **anything** other than functions up to the DB. This is intentional.

Changes to a function (that have been made locally) can be pushed to the database using 
```bash
tusk push -a # Push all functions

tusk push testing # Push all functions whose name starts with the word testing

tusk push public.% # Push all functions in the public schema

tusk push public.testing # Push all functions whose name starts with the word testing in the public schema
```

If there are any syntax errors in any of the functions then the push is rolled back and the error is highlighted to the user. 

By default if the function has unit tests defined, these unit tests will also be run. If the unit test fails with the new changes to the code then the push is rolled back and the DB will be left unchanged. This behaviour can be changed in the user_config.yaml file

### Running Unit Tests
Unit testing is perhaps the most exciting part of Tusk. Tusk allows the user to define unit tests as simple yaml files. These unit tests are run within a transaction and are rolled back at the completion of the tests. This allows the user to test two aspects of the function. Firstly it can test the outputs of the function. Secondly it can also test the side effects of the function, side effects being the tables that have rows inserted, updated or deleted.

To create a unit test first navigate to the directory of the function you wish to unit test. For example this could be ./schemas/YOUR_SCHEMA/functions/YOUR_FUNCTION

Create a unit_tests directory within this folder. Any yaml files placed within this unit_tests directory (works recursively so there can be additionaly folders within the unit_tests directory that also contain yaml files if you wish).

An example of how this might look is if we have a function that simply takes in two text items and outputs the concatenation of these two text items. Imagine this function is simply placed in the public schema. The function would be in this directory ./schemas/public/functions/concat/concat(text, text).sql

```sql
CREATE OR REPLACE FUNCTION public.concat(var1 text, var2 text)
    RETURNS text
    LANGUAGE plpgsql
AS $function$
BEGIN
    return var1 || var2;
END
$function$
```

A unit test can be written in the following file ./schemas/public/functions/concat/unit_tests/simple_test.yaml

Where the contents of the file could look like this:

```yaml
- name: 'Testing output of function'
  query: "SELECT public.concat('hello', 'world') AS res;"
  expected_output:
    - res: 'helloworld'
```

Additionally lets imagine that for whatever reason we insert into a table during our function concat. This insertion can also be tested. At the completion of the test the insertion will be rolled back and no longer exist in the table.

Using the same example as before. Lets imagine our function to be:

```sql
CREATE OR REPLACE FUNCTION public.concat(var1 text, var2 text)
    RETURNS text
    LANGUAGE plpgsql
AS $function$
BEGIN
    INSERT INTO public.people(name, age) VALUES ('Homer Simpson', 42);
    return var1 || var2;
END
$function$
```

Our unit test could look like this:
```yaml
- name: 'Testing output and side effect'
  query: "SELECT public.concat('hello', 'world') AS res;"
  expected_output:
    - res: 'helloworld'
  expected_side_effect:
    table_query: SELECT age FROM public.people WHERE name = 'Homer Simpson'
    expected_query_results:
    - age: 42
```

An example of a unit test that contains multiple rows and multiple columns for output is provided for completeness below:
```yaml
- name: 'Multiple Rows and Columns in Output'
  query: 'SELECT UNNEST(ARRAY[1,2]) num1, UNNEST(ARRAY[2,1]) num2;'
  expected_output:
  - num1: '1' # Row 1 Column 1
    num2: '2' # Row 1 Column 2
  - num1: '2' # Row 2 Column 1
    num2: '1' # Row 2 Column 2
- name: Multiple Rows and Columns in Side Effects
  query: INSERT INTO public.tusk_test(name, num) VALUES ('George', 17), ('George', 17);
  expected_output:
  expected_side_effect:
    table_query: SELECT name, num FROM public.tusk_test WHERE name = 'George';
    expected_query_results:
    - name: George # Row 1 Column 1
      num: 17      # Row 1 Column 2
    - name: George # Row 2 Column 1
      num: 17      # Row 2 Column 2
```

Running the unit tests is very similar to the previous commands. An example of this is as follows:
```bash
tusk test -a # Run all defined unit tests across all schemas

tusk test testing # Run unit tests for functions whose names start with the word testing

tusk test public.% # Run all tests defined in the public schema

tusk test public.testing # Run all tests in the public schema for functions whose name starts with the word testing
```

### PL/pgSQL Documentation

Tusk has built in documentation for PL/pgSQL functions. The documentation for these functions is generated from code comments within the functions themselves. This follows a standard very similar to JSDoc but definitely not as extensive. In order to generate the documentation for a function simply perform the following commands:

```bash
tusk doc -a # Generate the docs for every schema within the repo

tusk doc public # Generate the docs for the public schema

tusk doc pu # Generate the docs for any schemas that start with pu (ie. public)
```

To generate documentation for a function, the function must have comments of a very specific style. Firsly the doc comment must be wrapped in a block comment with the opening comment having two asterisk. For example:
```sql
/**


*/
```

To get a complete picture of what should be contained within the doc comments we will write doc comments for our previous example function concat. The doc comments could look like this:

```sql
CREATE OR REPLACE FUNCTION public.concat(var1 text, var2 text)
    RETURNS text
    LANGUAGE plpgsql
AS $function$
BEGIN

    /**
        This is the function description. It is parsed as any free text found before the first @ symbol.

        @author Homer Simpson
        @date 01/02/1234
        @example SELECT public.concat('Hello ', 'World');

        @param {TEXT} var1 The first part of the output
        @param {TEXT} var2 The second part of the output

        @return {TEXT} var1 and var2 concatenated together
    */

    INSERT INTO public.people(name, age) VALUES ('Homer Simpson', 42);
    return var1 || var2;
END
$function$
```

The information contained within the example above is the entirety of the JSDoc syntax that Tusk supports. That is Tusk supports a description and an author, date, example, param and return tags. 

The param tag indicates a parameter that is passed into the function. The param tag should be formatted as follows:

```sql
/**
  @param {THE PARAMETER TYPE} the_parameter_name The description of what the parameter does
*/
```

The return tag is very similar to the param tag just without a parameter name argument.

The example tag is intended to be used to show an example of how the function should be used. 

The above example concat function will generate the following markdown: 

````markdown
# concat

## concat(var1 text, var2 text)
- Author: Homer Simpson
- Date: 01/02/1234

### Description 
This is the function description. It is parsed as any free text found before the first @ symbol.

### Arguments

| Name | Type | Description                   |
| ---- | ---- | ----------------------------- |
| var1 | TEXT | The first part of the output  |
| var2 | TEXT | The second part of the output |

### Return Type

| Type | Description                         |
| ---- | ----------------------------------- |
| TEXT | var1 and var2 concatenated together |

### Example

```sql
SELECT public.concatenating('Hello ', 'World');
```

````




