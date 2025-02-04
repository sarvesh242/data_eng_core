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
  

  
