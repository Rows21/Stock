import rpy2.robjects as robjects
from rpy2.robjects import pandas2ri
pandas2ri.activate()


readRDS = robjects.r['readRDS']
df = readRDS('C:/Users/WR/Desktop/fm1.rds')
a=1