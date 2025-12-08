import os
import snowflake.connector as sc

#test

def read_secret(name):
    path = f"/run/secrets/{name}"
    if os.path.exists(path):
        with open(path) as f:
            return f.read().strip()
    return None

private_key_file = os.getenv("PRIVATE_KEY_FILE", "/run/secrets/snowflake_key.pem")
private_key_file_pwd = read_secret("snowflake_key_pwd")
 
conn_params = {
    'account': 'ABB-ABB_MO',
    'user': 'SNOW@us.abb.com',
    'authenticator': 'SNOWFLAKE_JWT',
    'private_key_file': private_key_file,
    'private_key_file_pwd': private_key_file_pwd,
    'warehouse': 'WH_BU_READ',
    'database': 'PROD_A3_MASTERDATA',
    'schema': 'V_ABB_PRODUCT_CID_DATA'
}
 
ctx = sc.connect(**conn_params)
cs = ctx.cursor()
 
# --- Test Query Execution ---
try:
    cs = ctx.cursor()
    test_query = """
        SELECT CURRENT_USER(), CURRENT_ACCOUNT(), CURRENT_ROLE();
        -- replace above with your actual query
    """
    cs.execute(test_query)
    results = cs.fetchall()
    for row in results:
        print(row)
finally:
    cs.close()
    ctx.close()