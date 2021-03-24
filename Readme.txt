O requisito para rodar esse script é:

1 - ter uma conta configurada com AWSCLI;

2 - Ter python e boto3 instalado;




------------ Comandos --------------

> python3 main.py -h ( para verificar os comandos );

usage: main.py [-h]  [--regions REGIONS] [--life-cycle] [--prefix PREFIX]

-h, --help -> show this help message and exit;

--regions REGIONS -> Filter regions to read buckets - e.g.: "us-east-1,us-east-2";

--life-cycle -> Boolean parameter to filter only buckets that have a life cycle rule;

--prefix PREFIX -> Prefix to filter only some files inside the bucket - e.g.:"logs/";


PS: Em relação ao custo, independe de qualquer filtro, é apenas o custo total do s3 no ultimo mês. Essa limitação é devido ao SDK que utilizei ( boto3 ).
