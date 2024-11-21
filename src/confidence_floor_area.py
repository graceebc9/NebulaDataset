import pandas as pd 
import numpy as np 

def calculate_floor_area_confidence(df, col1, col2):
    df['confidence_floor_area'] = np.where(
        df[col1].isna() | df[col2].isna(),
        'Not Applicable',
        None
    )
    
    valid_rows = ~df['confidence_floor_area'].eq('Not Applicable')
    
    if valid_rows.any():
        valid_df = df[valid_rows].copy()
        
        valid_df['floor_area_diff'] = abs(valid_df[col1] - valid_df[col2])
        valid_df['floor_area_pct_diff'] = (
            valid_df['floor_area_diff'] / 
            valid_df[[col1, col2]].max(axis=1) * 100
        )
        
        conditions = [
            (valid_df['floor_area_pct_diff'] <= 3),    
            (valid_df['floor_area_pct_diff'] <= 10),   
            (valid_df['floor_area_pct_diff'] <= 25)    
        ]
        choices = ['High', 'Medium', 'Low']
        
        valid_df['confidence_floor_area'] = np.select(
            conditions,
            choices,
            default='Very Low'
        )
        
        df.loc[valid_rows, 'confidence_floor_area'] = valid_df['confidence_floor_area']
    
    return df