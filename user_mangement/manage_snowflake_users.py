import yaml
import snowflake.connector


class user_mangement:
  '''This class handles all functionalities related to user management in Snowflake'''
  
  def __init__(self,filepath):
    ''' Constructor for the class user_mangement
        Args: 
          filepath: Path of the yml file containing the users'''
    self.filepath = filepath
    
  def load_users_from_json(self):
    '''Function to load all the users from the yml file.
       returns: A list containing all the users'''
    with open(,'rt') as fp:
      yaml_data = yaml.load(fp, Loader=yaml.FullLoader)
      print(yaml_data)
      list_of_users = yaml_data['users']
      # print(list_of_users)
      return list_of_users
  
  def get_existing_user_params(cursor, username):
    """Retrieve existing user parameters from Snowflake.
       Returns: A dict of user parameters or None"""
    cursor.execute(f"SHOW USERS LIKE '{username}'")
    users = cursor.fetchall()
    if not users:
        return None
    cursor.execute(f"DESCRIBE USER {username}")
    return {row[0].upper(): row[1] for row in cursor.fetchall()}
  
  def create_user(cursor, username, params):
    """Create a new user with the specified parameters.
       Returns: None"""
    param_str = ' '.join([f"{key} = '{value}'" for key, value in params.items()])
    cursor.execute(f"CREATE USER {username} {param_str}")
    print(f"User {username} created successfully.")

  def update_user(cursor, username, params, existing_params):
    """Update user parameters if they differ from existing values.
       Returns: None"""
    updates = []
    for key, value in params.items():
        if existing_params.get(key.upper()) != str(value):
            updates.append(f"{key} = '{value}'")
    
    if updates:
        update_query = f"ALTER USER {username} SET " + ', '.join(updates)
        cursor.execute(update_query)
        print(f"User {username} updated successfully.")
    else:
        print(f"No updates needed for user {username}.")

  def sync_users(config_file, conn):
    """Sync users based on the YAML configuration.
       Returns None"""
    config = load_config(config_file)
    cursor = conn.cursor()
    
    for user in config.get('users', []):
        username = user['name']
        user_params = {k.upper(): v for k, v in user.items() if k != 'name'}
        existing_params = get_existing_user_params(cursor, username)
        
        if existing_params is None:
            create_user(cursor, username, user_params)
        else:
            update_user(cursor, username, user_params, existing_params)
    
    cursor.close()
