import os
import pandas as pd

configfile: "config.yaml"

project_accession = config["project_accession"]
base_dir = config["base_directory"]
out_dir = config["output_directory"]
nextflow_path = config["nextflow_path"]
nextflow_merge_script = config["nextflow_merge_script"]
nextflow_run_script = config["nextflow_run_script"]

# 中間生成物・出力フォルダ作成
os.makedirs(base_dir, exist_ok=True)
os.makedirs(out_dir, exist_ok=True)

# samples.tsv を生成するルール
rule fetch_metadata:
    output: "samples.tsv"
    shell: """
    python scripts/get_samples.py {project_accession}
    """

# samples.tsvをパースして、sample -> ftp_urlsをマッピング
# ここではsamples.tsvを全読みして、サンプルごとのFTPファイル名を取得します。
samples_df = pd.read_csv("samples.tsv", sep="\t")
sample_to_ftp = samples_df.groupby("sample_accession")["ftp_url"].apply(list).to_dict()

# 全サンプルID
SAMPLES = list(sample_to_ftp.keys())

# 各サンプルは複数のFTPファイルを持つので、それらを`.gz`でフィルタ
def get_sample_gz(sample):
    return [url for url in sample_to_ftp[sample] if url.endswith(".gz")]

# ダウンロード先
def get_downloaded_files(sample):
    sample_dir = os.path.join(base_dir, sample)
    os.makedirs(sample_dir, exist_ok=True)
    gz_urls = get_sample_gz(sample)
    return [os.path.join(sample_dir, os.path.basename(url)) for url in gz_urls]

# マージ結果ファイル
def get_merged_file(sample):
    sample_dir = os.path.join(base_dir, sample)
    return os.path.join(sample_dir, "merged.fastq.gz")

# 最終解析出力ディレクトリ(ここではディレクトリターゲットとする)
def get_sample_output_dir(sample):
    return os.path.join(out_dir, sample)

# ダウンロードルール
rule download_fastq:
    input:
        "samples.tsv"
    output:
        expand("{base}/{sample}/{filename}", base=base_dir, sample=SAMPLES, filename=lambda wildcards: [os.path.basename(url) for url in get_sample_gz(wildcards.sample)])
    run:
        # Snakemakeはファイル毎に実行するので、inputはまとめて`output`が展開される
        # ここで実際にFTPからダウンロード
        # output は各ファイルへのパスなので、それに対応するFTP URLを検索してダウンロードする
        sample = wildcards.sample
        gz_urls = get_sample_gz(sample)
        out_files = expand("{base}/{sample}/{filename}", base=base_dir, sample=sample, filename=[os.path.basename(url) for url in gz_urls])
        for ftp_url, out_file in zip(gz_urls, out_files):
            shell("python scripts/download_ftp.py {ftp_url} {out_file}")

# マージルール: 各サンプルのFASTQをマージ
rule merge_fastq:
    input:
        lambda wildcards: get_downloaded_files(wildcards.sample)
    output:
        get_merged_file
    shell:
        """
        {nextflow_path} run {nextflow_merge_script} --input_dir {dirname} --output_file {output}
        """.format(
          nextflow_path=nextflow_path,
          nextflow_merge_script=nextflow_merge_script,
          dirname=lambda wildcards: os.path.join(base_dir, wildcards.sample),
          output="{output}"
        )

# 解析ルール: マージ済みファイルを解析
rule analyze_fastq:
    input:
        merged=get_merged_file
    output:
        directory(get_sample_output_dir("{sample}"))
    shell:
        """
        mkdir -p {output}
        {nextflow_path} run {nextflow_run_script} --input {input.merged} --output_dir {output}
        """.format(
          nextflow_path=nextflow_path,
          nextflow_run_script=nextflow_run_script
        )

# 最終的なターゲット
rule all:
    input:
        expand(directory(get_sample_output_dir(sample)), sample=SAMPLES)
