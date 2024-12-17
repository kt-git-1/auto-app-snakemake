import sys
import os
from ftplib import FTP
from urllib.parse import urlparse

ftp_url = sys.argv[1]
destination = sys.argv[2]

if not ftp_url.startswith('ftp://'):
    ftp_url = 'ftp://' + ftp_url

parse = urlparse(ftp_url)
ftp_server = parse.netloc
ftp_path = parse.path
filename = os.path.basename(ftp_path)

try:
    with FTP(ftp_server) as ftp:
        ftp.login()
        ftp.cwd(os.path.dirname(ftp_path))
        with open(destination, 'wb') as f:
            ftp.retrbinary('RETR ' + filename, f.write)
except Exception as e:
    print(f"Error downloading {ftp_url}: {e}")
    sys.exit(1)
