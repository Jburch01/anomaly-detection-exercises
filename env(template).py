# This is the env template file. Make sure you put in your own username, password and the host for file to work. Rename the file to env.py when done. DOES NOT WORK IF NOT RENAMED 

username = your_username_goes_here
password = your_password_goes_here
host = the_hostname_goes_here

def get_db_url(db):
    return f'mysql+pymysql://{username}:{password}@{host}/{db}'