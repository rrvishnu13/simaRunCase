import h5py
import numpy as np
import pandas as pd
import sys

def decode_attr(attr):
    return attr.decode('utf-8') if isinstance(attr, bytes) else str(attr)


def flatten_h5(h5obj, path=''):
    datasets = {}
    for key in h5obj.keys():
        item = h5obj[key]
        item_path = f"{path}/{key}" if path else key
        if isinstance(item, h5py.Dataset):
            datasets[item_path] = item
        elif isinstance(item, h5py.Group):
            datasets.update(flatten_h5(item, path=item_path))
    return datasets

def h52pq(h5File):


    df_dict = {}

    with h5py.File(h5File, "r") as res:
        
        all_datasets = flatten_h5(res) #get flattened database
        
        for full_key, tsData in all_datasets.items():
            
            #clean_up file names --> Keep only things after Dynamic
            key_split = full_key.split('/')
            
            if 'Dynamic' in key_split:
                dynIn = key_split.index('Dynamic')
                key_name = '/'.join(key_split[dynIn + 1:])
            
            else:
                continue

            tsData = res[full_key]
            dt = tsData.attrs['delta'] #storage spacing
            t0 = tsData.attrs['start'] #initial time --> need not be 0
            unit = decode_attr(tsData.attrs.get('yunit', '')).replace('*','.') # yunit if exists
            tSeries = tsData[()] #convert h5 to numpy array
            
            n = tSeries.shape[0]
            t = np.linspace(t0, t0 + (n - 1) * dt, n)
            
            df = pd.DataFrame({'t': t, key_name + f'_[{unit}]' : tSeries}).set_index('t')
            df_dict[key_name] = df

    # Determine common start, end, and smallest dt
    t_min = min(df.index[0] for df in df_dict.values())
    t_max = max(df.index[-1] for df in df_dict.values())
    dt_min = min(np.diff(df.index).min() for df in df_dict.values())

    t_common = np.arange(t_min, t_max + dt_min, dt_min)
    index = pd.Index(t_common, name='t')

    # resample all series on common index
    aligned = []
    for name, df in df_dict.items():
        df_re = df.reindex(index).interpolate().bfill().ffill()
        aligned.append(df_re)

    df_common = pd.concat(aligned, axis=1)

    df_common = df_common.reset_index()  # index becomes a column
    df_common.rename(columns={'index': 'Time_[s]'}, inplace=True)

    df_common.to_parquet(h5File.replace('.h5', '.parquet'))


if __name__ == "__main__":
    
    if len(sys.argv) != 2:
        print("Usage: python h52pq <input_file.h5>")
        sys.exit(1)

    input_file = sys.argv[1]
    h52pq(input_file)