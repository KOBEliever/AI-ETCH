import pandas as pd
import re

def split_zone_data_x(df: pd.DataFrame) -> pd.DataFrame:
    area_col = [col for col in df.columns if re.search(r'AREA\d+', col)]
    other_col = [col for col in df.columns if col not in area_col]

    if not area_col:
        print("无AREA相关列")
        return df
    
    print(f"找到{len(area_col)}个AREA相关列，开始划分ZONE")

    metrics = set()

    for col in area_col:
        metric = re.sub(r'_\d+$', '', col)
        metric = re.sub(r'AREA\d+', '', metric).replace('__', '_')
        metrics.append(metric)

    dfs_to_concat = []

    for metric in metrics:
        metric_find = metric.split('_')
        metric_polish = metric_find[0] if metric_find else ''
        metric_para = metric[len(metric_polish) + 1:] if len(metric_polish) > 0 else metric

        if 'AB_PRS' in metric_para:
            metric_cols = [col for col in area_col if metric_polish in col and 'AB_PRS' in col and 'IL' not in col]
        else:
            metric_cols = [col for col in area_col if metric_polish in col and metric_para in col]

        if not metric_cols:
            continue

        melted = df[metric_cols + ['WAFER_ID']].melt(
            id_vars=["WAFER_ID"],
            var_name='original_col',
            value_name=metric
        )

        melted['AREA'] = melted['original_col'].str.extract(r'AREA(\d+)').astype(int)

        melted = melted.set_index(['WAFER_ID', 'AREA'])[[metric]]

        dfs_to_concat.append(melted)
    
    if not dfs_to_concat:
        print("划分失败")
        return df
    
    try:
        area_data = pd.concat(dfs_to_concat, axis=1).reset_index()
    except Exception as e:
        dfs_clean = [d.copy() for d in dfs_to_concat]
        area_data = pd.concat(dfs_clean, axis=1).reset_index()

    other_data = df[other_col].copy()
    df = pd.merge(area_data, other_data, on=["WAFER_ID"], how="inner")

    for i in range(1,9):
        df[f'AREA{i}'] = 0

    for i in range(1,9):
        df.loc[df["AREA"] == i, f'AREA{i}'] = 1
    
    return df
