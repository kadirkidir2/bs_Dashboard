from google.analytics.admin_v1alpha import AnalyticsAdminServiceClient
from google.oauth2 import service_account

SERVICE_ACCOUNT_INFO = {
  "type": "service_account",
  "project_id": "roboskop-test",
  "private_key_id": "660cf19cbdf9c7e699d9504364c876d0286b8f84",
  "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQCx9DTmiykMgNRY\noyjgZIBjvUdFniKhswpRmzGcYkoRt7zAgaD1ns3TPAeHRecIw9NmvfFTRxmWjDDy\nnNhaHM0yfG1ydx2En0gr8yW0UAGhTnBDXnA5iCotG2wBjLpk9S0MQcHL65YcC2m4\nYyRTubd1dCV9gI0qe9PMeTa45NotCzQTd/JgwuAq4XdguusYikxrGM7n9NGv3Q4q\nb6G2xAiFW3JOvqZX9kT8tKP1b/bXjVqw4mpSHRtcbtFeySv2R29xSIJw035T4Wtc\no7B69tpP99wSfztvfAkYkj4Rwqc9dmPUFIAbv8bHI6he/zG2W/N2hdQri6lKS89I\n0xn77up1AgMBAAECggEABaD832H9fWjBzr/ydg6XxQ/ipkAXRlgcdnJbQmAVibPv\nI4O2LK7GQe0/K7c5VJylHjXZ4VF5bLqofbQaX9dTYkpvR9SeuWg0Zzd8tNRnCMwr\nXuYo1JkLBHw+kVn7N2BN+XpHAAVGrUJrKKryJjfUpsFywiKD1JkwHOKkqXqLsCsB\n+Ti4hP4sGyJ58Hz5/iHTPwVNoMu3IwS8HMLPn+dz0eA5dpSEIFAQYtMekOWuQeqN\nSgYGqQ2EuPHrVkviH9HAU9SO0Ub0u7PF4woVu2ZYWdqQ5kvEfNWzUfMn6ET4bmZX\nO8jXHwP5gw7By7+Hm1YvCT2kNwiZJ148Df4VVwTYLQKBgQDtgmBabQi/uKbCSyrb\nqbX38wfKY5F/GkImLcCVIYVRq5cyhtsCJ5EinUjJ2qy3dLijkJw9nNt4qIiB1ZyO\nSzFuBthr/OSv9LapUYSG1G98j4F2lcdEd0byx2gDMZoSj95/MckYbzof93/jZgLM\n4U393CtH+vfd6/AZi9J+At2qlwKBgQC/zuBNBweq94eCtCM0OGRGiIvvonpM78U/\ne2WGAuWWNUTi8EYZ5gdwzeJ648cuJZonjoOW5uDYXWApuHHX3righpu5XCBy5+8p\nbgrwKqf/v9rzJnA+BuE3dMunAKbzWeTAMC2nqS0K1gcy8Sppqjp+gPQ9UXMLtlxG\ncYgKHZkw0wKBgQDh6Y4+1NFqk3fC/X6RnDai1v8FCno9fLuI1yIEd/L4wQ87FzzA\nKuSJGTRAFAkPIy3xHr8Od+HmPeBCJu4YBvvIXSiEZvMbTthj6070dKJqx5FsO4Fj\n5VSN3xBXYVnc6A3JzJAr9rhB6cgygziMqo3ltwNQHy6fXyltEysvgwhXlwKBgDQ1\nrKpq4nRwIku6muaI/wqP+n9FG8M7dNZ90Tm0KihD4bdgLcS474eOEzreK4ZtJ9Pi\noAEAYW2jpRxWH27iKiFP4q2G3TJJ3bjKQmHzSn18DK4o5V6M4tsEiLaxn8AX1QNX\nWeYMT71mWtlL+f5/HqL2mQTj2cvzXJS0LJKMUfmPAoGBAJ+nZjPOdAWiYb+48R+w\nQnGNpjSiEfBMthgMS/nI4cdbiW16L+Syj5oMaonJ1qVpCvtzxjvy/8eEgM/qnopi\nB9sGWfrpKQhl7o4yZU5rAU6IDByCjqq+YFKDaiS8obVCSLZqDkapemPJPmUzaHUP\nmSXlNEcSTPDDC/gKwnJukyNM\n-----END PRIVATE KEY-----\n",
  "client_email": "roboskop-service@roboskop-test.iam.gserviceaccount.com",
  "client_id": "111307522619497186177",
  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
  "token_uri": "https://oauth2.googleapis.com/token",
  "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
  "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/roboskop-service%40roboskop-test.iam.gserviceaccount.com",
  "universe_domain": "googleapis.com"
}

def quick_find():
    credentials = service_account.Credentials.from_service_account_info(
        SERVICE_ACCOUNT_INFO,
        scopes=['https://www.googleapis.com/auth/analytics.readonly']
    )
    
    client = AnalyticsAdminServiceClient(credentials=credentials)
    accounts = client.list_accounts()
    
    for account in accounts:
        print(f"Hesap: {account.display_name}")
        properties = client.list_properties(parent=account.name)
        for prop in properties:
            print(f"  Property: {prop.display_name} - ID: {prop.name.split('/')[-1]}")

quick_find()