import sys
import requests

project_accession = sys.argv[1]

url = f"https://www.ebi.ac.uk/ena/portal/api/filereport?accession={project_accession}&result=read_run&fields=sample_accession,submitted_ftp&format=tsv"

response = requests.get(url, timeout=10)
response.raise_for_status()
data = response.text.strip().split('\n')
# ヘッダをスキップして出力: sample_accession  ftp_url
with open("samples.tsv", "w") as f:
    f.write("sample_accession\tftp_url\n")
    for line in data[1:]:
        f.write(line + "\n")
