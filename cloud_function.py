import requests
import sqlalchemy
import sendgrid
import os
from sendgrid.helpers.mail import *
from google.cloud import secretmanager

project_id = os.environ["GCP_PROJECT"]

client = secretmanager.SecretManagerServiceClient()
name = f"projects/{project_id}/secrets/fan_db_password/versions/latest"
response = client.access_secret_version(name=name)

db_name = "functionDB"
db_user = "root"
db_hostname = "ip_addr"
db_port = 3306
db_password = response.payload.data.decode("UTF-8")
driver_name = "mysql+pymysql"
endpoints = ["endpoints1"]

db = sqlalchemy.create_engine(
    sqlalchemy.engine.url.URL(
        drivername=driver_name,
        username=db_user,
        password=db_password,
        host=db_hostname,
        port=db_port,  
        database=db_name,
    ),
    pool_size=5,
    max_overflow=2,
    pool_timeout=30,
    pool_recycle=1800,
)


def my_healthcheck(request):
    request_json = request.get_json()
    insert_into_db_statuses(request)
    delete_db_old_data(request)
    check_http(request)

    return "ok"


def insert_into_db_statuses(self):

    for url in endpoints:
        stat_code = str(requests.get(url).status_code)
        with db.connect() as conn:
            conn.execute(
                sqlalchemy.text(
                    "INSERT INTO health (`Id`, `url`, `code`, `DLM`) VALUES (UUID(), '"
                    + url
                    + "',"
                    + stat_code
                    + ", NOW(3));"
                )
            )


def delete_db_old_data(self):
    for url in endpoints:
        with db.connect() as conn:
            conn.execute(
                sqlalchemy.text(
                    "CREATE TEMPORARY TABLE temp (Select * from health where url='"
                    + url
                    + "' order by DLM desc Limit 3);"
                )
            )
            conn.execute(
                sqlalchemy.text(
                    "DELETE FROM health WHERE url='"
                    + url
                    + "' and id not in (SELECT Id from temp);"
                )
            )
            conn.execute(sqlalchemy.text("DROP TABLE temp;"))


def get_qty_unhealth_responces(my_url):
    with db.connect() as conn:
        result = conn.execute(
            sqlalchemy.text(
                "Select count(*) from health where url ='"
                + my_url
                + "' and (code like '4%' or code like '5%');"
            )
        )
    for row in result:
        return str(row)[1:].strip(",)")


def sent_email(my_endpoint):
    sg = sendgrid.SendGridAPIClient(api_key=os.environ.get("EMAIL_API_KEY"))
    from_email = Email("email")
    to_email = To("email")
    subject = "Elasticsearch healthcheck"
    content = Content(
        "text/plain", "Emmergency! Instance " + my_endpoint + " is unhealthy now!"
    )
    mail = Mail(from_email, to_email, subject, content)
    response = sg.client.mail.send.post(request_body=mail.get())


def check_http(self):
    for url in endpoints:
        qty_unhealth_responces = int(get_qty_unhealth_responces(url))
        if qty_unhealth_responces == 3:
            sent_email(url)
