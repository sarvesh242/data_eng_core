CREATE OR REPLACE PROCEDURE manage_user_proc(
    user_name STRING, 
    user_password STRING, 
    default_role STRING, 
    must_change_password BOOLEAN
)
RETURNS STRING
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
DECLARE user_exists INT;
BEGIN
    -- Check if the user already exists
    SELECT COUNT(*) INTO user_exists FROM TABLE(SNOWFLAKE.INFORMATION_SCHEMA.USERS)
    WHERE NAME = user_name;
    
    IF user_exists = 0 THEN
        -- Create new user
        EXECUTE IMMEDIATE 'CREATE USER ' || user_name || 
        ' PASSWORD = ''' || user_password || '''' ||
        ' DEFAULT_ROLE = ' || default_role || 
        ' MUST_CHANGE_PASSWORD = ' || must_change_password;
        RETURN 'User Created: ' || user_name;
    
    ELSE
        -- Update user details
        EXECUTE IMMEDIATE 'ALTER USER ' || user_name || 
        ' SET PASSWORD = ''' || user_password || '''' ||
        ', DEFAULT_ROLE = ' || default_role || 
        ', MUST_CHANGE_PASSWORD = ' || must_change_password;
        RETURN 'User Updated: ' || user_name;
    END IF;
END;
$$;
