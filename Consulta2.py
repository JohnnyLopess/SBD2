from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col

# Caminho completo para os arquivos JAR baixados
jars = "/home/adduser/hadoop-aws-3.2.0.jar,/home/adduser/aws-java-sdk-bundle-1.11.375.jar,/home/adduser/hadoop-common-3.2.0.jar"

# Configurar a sessão do Spark
spark = SparkSession.builder \
    .appName("ConsultaMinIO") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://000.000.00.00:9000") \ # Aqui se deve trocar pelo IP do MinIO
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "password") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
    .config("spark.jars", jars) \
    .config("spark.ui.showConsoleProgress", "false") \
    .getOrCreate()

# Caminho para os arquivos CSV no MinIO
bucket_name = "mentalheath"
file_prefix = "dados/"

# Lista de arquivos CSV para leitura
csv_files = {
    "Answer": f"s3a://{bucket_name}/{file_prefix}Answer.csv",
    "Question": f"s3a://{bucket_name}/{file_prefix}Question.csv",
    "Survey": f"s3a://{bucket_name}/{file_prefix}Survey.csv"
}

# Função para ler os arquivos CSV
def read_csv(file_path):
    return spark.read.csv(file_path, header=True, inferSchema=True)

# Ler os arquivos CSV
df_answer = read_csv(csv_files["Answer"])
df_question = read_csv(csv_files["Question"])
df_survey = read_csv(csv_files["Survey"])

# Registrar as tabelas como temporárias para consulta SQL
df_answer.createOrReplaceTempView("ANSWER")
df_question.createOrReplaceTempView("QUESTION")
df_survey.createOrReplaceTempView("SURVEY")

# Realizar a consulta SQL
query = """
SELECT q.questiontext AS QuestionText, AVG(CAST(a.AnswerText AS DOUBLE)) as mediaIdade
FROM QUESTION q
JOIN ANSWER a ON q.questionid = a.QuestionID
WHERE a.SurveyID = 2014 AND q.questionid = 1
GROUP BY q.questiontext
"""

result = spark.sql(query)
result.show()

# Parar a sessão do Spark
spark.stop()
