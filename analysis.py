import numpy as np
import pandas as pd

def algo1(in4,in5,param1,param3):

    out1 = 0.00045 * in4 + 4.5 * param1 / (in5 * 0.5) + 4.5 * param3 / (in5 * 0.5) + 500 * (param5 + param6) + 0.041 * param7 + 0.01 * param8 + 6.1*[param9]/[in5]*0.5 + param10
    out2 = np.median(out1)

    return out1, out2

def algo2():
    return out1, out2

if __name__ == '__main__':
    df = pd.read_csv('api.csv').iloc[:,1:]
    out1, out2 = algo1()