import numpy as np
import pandas as pd
import ray

from ..utils.decorators import useray

@ray.remote
def compute_factor(name, factor, df):
    s = factor.compute(df)
    frame = pd.DataFrame()
    frame[name] = s
    return frame

@ray.remote
def concat(df1,df2):
    return pd.concat([df1, df2], axis=1)

@useray
def compute(exp_dict, df):
    # Computing and stacking factors
    frames = []
    for k, exp in exp_dict.items():
        frames.append(compute_factor.remote(k,exp,df))
        if len(frames)>3:
            ray.wait(frames)
    cnt=0
    while len(frames) > 1:
        frames = frames[2:] + [concat.remote(frames[0],frames[1])]
        if cnt>3:
            ray.wait(frames)
        else:
            cnt+=1

    # Filling nans with zeros if previous non nan value
    res = ray.get(frames[0])
    res = res.unstack()
    for (column, data) in res.iteritems():
        nans = np.isnan(data)
        for i in range(1,len(data)):
            if nans[i] and not nans[i-1]:
                data[i] = 0
                nans[i] = False
    res.dropna(inplace=True)

    return res.stack()
