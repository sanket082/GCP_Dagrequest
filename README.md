DAG trigger with Airflow


Step 1: Create a cloud storage bucket (Test1).


Step 2: Create a Composer environment.  In Composer 1 create an env2 with location us-central1.
 It contains the airflow UI to check the dags in the composer environment.


Step 3: Create a function with trigger set for bucket on finalizing /creating and select bucket test1.
 










Step 4: Obtaining Client id
To get Client ID we create a new cloud function client_id with Https trigger and copy code from client_id.py.

Replace PROJECT_ID with your project id, YOUR_LOCATION with location of your composer and COMPOSER_NAME with name of your composer environment. Next we give permissions to cloud function to access the clien_id


```

import google.auth
import google.auth.transport.requests
import requests
import six.moves.urllib.parse



def hello_world(request):
    """Responds to any HTTP request.
    Args:
        request (flask.Request): HTTP request object.
    Returns:
        The response text or any set of values that can be turned into a
        Response object using
        `make_response <http://flask.pocoo.org/docs/1.0/api/#flask.Flask.make_response>`.
    """

    credentials, _ = google.auth.default(
        scopes=["https://www.googleapis.com/auth/cloud-platform"]
    )
    authed_session = google.auth.transport.requests.AuthorizedSession(credentials)

 project_id = '<PROJECT_ID>'
location = '<YOUR_LOCATION>’'
composer_environment = ‘<COMPOSER_NAME>’

    environment_url = (
        "https://composer.googleapis.com/v1beta1/projects/{}/locations/{}"
        "/environments/{}"
    ).format(project_id, location, composer_environment)
    composer_response = authed_session.request("GET", environment_url)
    environment_data = composer_response.json()
    composer_version = environment_data["config"]["softwareConfig"]["imageVersion"]
    if "composer-1" not in composer_version:
        version_error = ("This script is intended to be used with Composer 1 environments. "
                        "In Composer 2, the Airflow Webserver is not in the tenant project, "
                        "so there is no tenant client ID. "
                        "See https://cloud.google.com/composer/docs/composer-2/environment-architecture for more details.")
        raise (RuntimeError(version_error))
    airflow_uri = environment_data["config"]["airflowUri"]

    # The Composer environment response does not include the IAP client ID.
    # Make a second, unauthenticated HTTP request to the web server to get the
    # redirect URI.
    redirect_response = requests.get(airflow_uri, allow_redirects=False)
    redirect_location = redirect_response.headers["location"]

    # Extract the client_id query parameter from the redirect.
    parsed = six.moves.urllib.parse.urlparse(redirect_location)
    query_string = six.moves.urllib.parse.parse_qs(parsed.query)
    print(query_string["client_id"][0])
    return query_string["client_id"][0]
    # request_json = request.get_json()
    # if request.args and 'message' in request.args:
    #     return request.args.get('message')
    # elif request_json and 'message' in request_json:
    #     return request_json['message']
    # else:
    #     return f'Hello World!'

```



Now go to cloud function tab and sect client_id function and select permissions.
 

Now click on add permissions and add allUsers in New Principle, also add cloud function invoker role in Role tab and click save.
 
 


Now go to cloud function trigger and use the cloud trigger url. You will find your client_id.


Step 5: Finding webserver id.
Go to your Airflow UI from composer. The webserver_id the part of url before the .appspot.com.
 

Step 6:  Press save and press next. For code change to Python 3.9 and copy code for main.py and requirements.txt from file sent previously. 

Replace YOUR_CLIENT_ID with the client id obtained in step 4 and YOUR_WEBSERVER_ID with
Webserver id obtained in step 5.

```
import datetime

import airflow
from airflow.operators.bash_operator import BashOperator
from google.auth.transport.requests import Request
from google.oauth2 import id_token
import requests

def make_iap_request(url, client_id, method='GET', **kwargs):
    if 'timeout' not in kwargs:
        kwargs['timeout'] = 180
    google_open_id_connect_token = id_token.fetch_id_token(Request(), client_id)

    resp = requests.request(
        method, url,
        headers={'Authorization': 'Bearer {}'.format(
            google_open_id_connect_token)}, **kwargs)
    print(resp)
    if resp.status_code == 403:
        raise Exception('Service account does not have permission to '
                        'access the IAP-protected application.')
    elif resp.status_code != 200:
        raise Exception(
            'Bad response from application: {!r} / {!r} / {!r}'.format(
                resp.status_code, resp.headers, resp.text))
    else:
        return resp.text




IAM_SCOPE = 'https://www.googleapis.com/auth/iam'
OAUTH_TOKEN_URI = 'https://www.googleapis.com/oauth2/v4/token'
USE_EXPERIMENTAL_API = False


def hello_gcs(event, context):
  client_id = ‘<YOUR_CLIENT_ID>’
  webserver_id = ‘<YOUR_WEBSERVER_ID'
  dag_name = 'update_bigquery'
  if USE_EXPERIMENTAL_API:
        endpoint = f'api/experimental/dags/{dag_name}/dag_runs'
        json_data = {'conf': event, 'replace_microseconds': 'false'}
  else:
      endpoint = f'api/v1/dags/{dag_name}/dagRuns'
      json_data = {'conf': event}
  webserver_url = (
      'https://'
      + webserver_id
      + '.appspot.com/'
      + endpoint
  )
  make_iap_request(
        webserver_url, client_id, method='POST', json=json_data)
  print(webserver_url)

```

Press Deploy.










Step 7: Setting up service account

Open Google Active cloud shell and run the following commands in grey.

```
#Copy project id from cloud home page

export PROJECT= <REPLACE-THIS-WITH-YOUR-PROJECT-ID>
 
#setting PROJECT variable 

gcloud config set project $PROJECT

# Create a Service Account for POST Trigger

gcloud iam service-accounts create dag-trigger

# Give service account permissions to create tokens for iap requests.

gcloud projects add-iam-policy-binding $PROJECT \
--member \
serviceAccount:dag-trigger@$PROJECT.iam.gserviceaccount.com \
--role roles/iam.serviceAccountTokenCreator

gcloud projects add-iam-policy-binding $PROJECT \
--member \
serviceAccount:dag-trigger@$PROJECT.iam.gserviceaccount.com \
--role roles/iam.serviceAccountActor


# Service account also needs to be authorized to use Composer.

gcloud projects add-iam-policy-binding $PROJECT \
--member \
serviceAccount:dag-trigger@$PROJECT.iam.gserviceaccount.com \
--role roles/composer.user




#setting service account key to trigger the dag.


gcloud iam service-accounts keys create ~/$PROJECT-dag-trigger-key.json \
--iam-account=dag-trigger@$PROJECT.iam.gserviceaccount.com


# Export keys to projects key folder.

export GOOGLE_APPLICATION_CREDENTIALS=~/$PROJECT-dag-trigger-key.json
```

Step 8: Open DAGs folder of the composer environment and upload dag.py file there.

Replace PROJECT_ID with your project’s ID in the dag.py
```

import airflow
from datetime import datetime
from google.cloud import storage, bigquery
from airflow.operators.python_operator import PythonOperator

def process_data(**kwargs):
    # Extract the data from the kwargs dictionary
    data = kwargs['dag_run'].conf
    client = bigquery.Client()
    now = datetime.now()
    table_id = "<PROJECT_ID>.task_creation.task_table"

    rows_to_insert = [
        {u"file_name": data['name'],u"timestamp":now.strftime("%m/%d/%Y, %H:%M:%S")},
       
    ]

    errors = client.insert_rows_json(table_id, rows_to_insert)  
    if errors == []:
        print("New rows have been added.")
    else:
        print("Encountered errors while inserting rows: {}".format(errors))
    pass

default_args = {
    'owner': 'me',
    'start_date': datetime(2022, 1, 1),
}

dag = airflow.DAG(
    'update_bigquery',
    default_args=default_args,
    start_date = datetime(2023, 1, 1),
    schedule_interval=None,
)

process_data_task = PythonOperator(
    task_id='process_data',
    python_callable=process_data,
    provide_context=True,
    dag=dag,
)



```

  








Step 9: Check airflow by clicking Airflow web UI in composer Environment configuration tab and check if airflow update_bigquery is in the airflow UI.
 
 









Step 10: Setting up BigQuery schema and table. Open bigquery tab in GCP and click on create dataset. Put Dataset ID as task_creation. 
 

In task_creation dataset create a table name task_table.
 








Step 11:  Open the table and click on edit schema. Add field file_name and timestamp as fields with type String
 





















Results 

Now when you upload a file to Test1 bucket it will trigger the cloud function which inturn will call the airflow DAG and add the name and the time of upload to the bigquery  table.  
