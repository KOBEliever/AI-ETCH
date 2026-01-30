"""列重命名映射器 - 参考RenameColProcessor逻辑"""
from typing import Dict, Any
import pandas as pd
from utils.logger import get_logger
from .base_mapper import BaseMapper, MapperFactory

logger = get_logger(__name__)


class ColumnRenameMapper(BaseMapper):
    """
    列重命名映射器
    
    功能：
    1. 根据para_info表将不同模块（PolishA/B/C/D）的原始列名重命名
    2. 合并AB、CD模块生成POLISH标识
    """
    
    def __init__(self, config: Dict[str, Any], db_manager):
        super().__init__(config, db_manager)
        
        # 基础列（不需要重命名的列）
        self.base_col = [
            'PROC_EQP', 'PRODUCT', 'RECIPE', 
            'PRE_GLB_RAW_VALUE', 'PST_GLB_RAW_VALUE', 
            'PRE_M1_TIME', 'PST_M1_TIME', 
            'LOT_ID', 'X_TIME', 'LOT'
        ]
        
        # 从数据库或配置加载para_info
        self.para_info = self._load_para_info()
    
    def _load_para_info(self) -> pd.DataFrame:
        """
        加载参数映射信息
        
        Returns:
            para_info DataFrame
        """
        try:
            if self.db is not None and hasattr(self.db, 'get_para_info'):
                # 从数据库加载（异步方法需要同步包装）
                logger.info("从数据库加载para_info")
                return self.db.get_para_info()
            elif 'para_info' in self.config:
                # 从配置加载
                logger.info("从配置加载para_info")
                return pd.DataFrame(self.config['para_info'])
            else:
                # 返回空DataFrame
                logger.warning("未配置para_info，返回空映射")
                return pd.DataFrame(columns=['PARA_NAME_WITH_CODE', 'FROM_SRC_FIELD', 'PARA_NAME'])
        except Exception as e:
            logger.error(f"加载para_info失败: {e}")
            return pd.DataFrame(columns=['PARA_NAME_WITH_CODE', 'FROM_SRC_FIELD', 'PARA_NAME'])
    
    async def process(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        处理数据：重命名列并合并模块
        
        Args:
            data: 输入数据（需包含MODULE列）
            
        Returns:
            重命名并合并后的数据
        """
        if not self.validate_input(data):
            return data
        
        if "MODULE" not in data.columns:
            logger.warning("缺少MODULE列，跳过重命名处理")
            return data
        
        # 拆分不同模块的数据
        module_series = data["MODULE"].fillna("").astype(str)
        
        df_PolishA = data[module_series.str.contains("PolishA", case=False, na=False)].copy()
        df_PolishB = data[module_series.str.contains("PolishB", case=False, na=False)].copy()
        df_PolishC = data[module_series.str.contains("PolishC", case=False, na=False)].copy()
        df_PolishD = data[module_series.str.contains("PolishD", case=False, na=False)].copy()
        
        logger.info(
            f"拆分模块数据 - A:{len(df_PolishA)}, B:{len(df_PolishB)}, "
            f"C:{len(df_PolishC)}, D:{len(df_PolishD)}"
        )
        
        # 删除MODULE列
        for df in (df_PolishA, df_PolishB, df_PolishC, df_PolishD):
            df.drop(columns=["MODULE"], inplace=True, errors="ignore")
        
        # 重命名列
        df_polishA_renamed = await self._rename_columns_polishAC(df_PolishA)
        df_polishB_renamed = await self._rename_columns_polishBD(df_PolishB)
        df_polishC_renamed = await self._rename_columns_polishAC(df_PolishC)
        df_polishD_renamed = await self._rename_columns_polishBD(df_PolishD)
        
        # B和D删除基础列（避免合并时重复）
        df_polishB_renamed = df_polishB_renamed.drop(columns=self.base_col, errors='ignore')
        df_polishD_renamed = df_polishD_renamed.drop(columns=self.base_col, errors='ignore')
        
        # 合并AB、CD
        result_dfs = []
        
        if not df_polishA_renamed.empty and not df_polishB_renamed.empty:
            df_PolishAB = pd.merge(
                df_polishA_renamed, 
                df_polishB_renamed, 
                on='WAFER_ID', 
                how='inner'
            )
            df_PolishAB['POLISH'] = 'AB'
            result_dfs.append(df_PolishAB)
            logger.info(f"合并AB模块: {len(df_PolishAB)} 行")
        
        if not df_polishC_renamed.empty and not df_polishD_renamed.empty:
            df_PolishCD = pd.merge(
                df_polishC_renamed, 
                df_polishD_renamed, 
                on='WAFER_ID', 
                how='inner'
            )
            df_PolishCD['POLISH'] = 'CD'
            result_dfs.append(df_PolishCD)
            logger.info(f"合并CD模块: {len(df_PolishCD)} 行")
        
        if not result_dfs:
            logger.warning("没有可合并的模块数据")
            return pd.DataFrame()
        
        # 合并所有结果
        merged = pd.concat(result_dfs, ignore_index=True)
        logger.info(f"列重命名完成，输出: {merged.shape}")
        
        return merged
    
    async def _rename_columns_polishAC(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        重命名PolishA/C的列（映射到P1前缀）
        
        Args:
            df: 输入DataFrame
            
        Returns:
            重命名后的DataFrame
        """
        if df.empty or self.para_info.empty:
            return df
        
        try:
            rename_map = self.para_info[['PARA_NAME_WITH_CODE', 'FROM_SRC_FIELD']]
            P1_mapping = rename_map[rename_map['PARA_NAME_WITH_CODE'].str.contains('P1_', na=False)]
            P1_mapping = P1_mapping.dropna(subset=['FROM_SRC_FIELD'])
            rename_dict = dict(zip(P1_mapping['FROM_SRC_FIELD'], P1_mapping['PARA_NAME_WITH_CODE']))
            
            df = df.rename(columns=rename_dict)
            logger.debug(f"P1列重命名: {len(rename_dict)} 个列")
            
        except Exception as e:
            logger.error(f"P1列重命名失败: {e}")
        
        return df
    
    async def _rename_columns_polishBD(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        重命名PolishB/D的列（映射到P2前缀）
        
        Args:
            df: 输入DataFrame
            
        Returns:
            重命名后的DataFrame
        """
        if df.empty or self.para_info.empty:
            return df
        
        try:
            rename_map = self.para_info[['PARA_NAME_WITH_CODE', 'FROM_SRC_FIELD']]
            P2_mapping = rename_map[rename_map['PARA_NAME_WITH_CODE'].str.contains('P2_', na=False)]
            P2_mapping = P2_mapping.dropna(subset=['FROM_SRC_FIELD'])
            rename_dict = dict(zip(P2_mapping['FROM_SRC_FIELD'], P2_mapping['PARA_NAME_WITH_CODE']))
            
            df = df.rename(columns=rename_dict)
            logger.debug(f"P2列重命名: {len(rename_dict)} 个列")
            
        except Exception as e:
            logger.error(f"P2列重命名失败: {e}")
        
        return df

    async def _cal_thk_from_raw_value(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        根据PRE_GLB_RAW_VALUE和PST_GLB_RAW_VALUE计算PRE_GLB_STI_THK_70000和PST_GLB_CMP_THK_80000
        
        Args:
            df: 输入DataFrame
            
        Returns:
            重命名后的DataFrame
        """
        if df.empty:
            return df
        
        try:
            df['PRE_VALUE_LIST'] = df['PRE_GLB_RAW_VALUE'].fillna('').str.split(',')
            df['PRE_VALUE_LIST'] = df['PRE_VALUE_LIST'].apply(lambda x: [float(i) for i in x if i != ''])

            df['PST_VALUE_LIST'] = df['PST_GLB_RAW_VALUE'].fillna('').str.split(',')
            df['PST_VALUE_LIST'] = df['PST_VALUE_LIST'].apply(lambda x: [float(i) for i in x if i != ''])

            df['PRE_GLB_STI_THK_70000'] = df['PRE_VALUE_LIST'].apply(lambda x: np.mean(x) if x else np.nan)
            df['PST_GLB_CMP_THK_80000'] = df['PST_VALUE_LIST'].apply(lambda x: np.mean(x) if x else np.nan)
            
            df = df.drop(columns=['PRE_VALUE_LIST', 'PST_VALUE_LIST'], errors='ignore')
            logger.debug(f"PRE_GLB_STI_THK_70000和PST_GLB_CMP_THK_80000计算完成")
            return df
        except Exception as e:
            logger.error(f"PRE_GLB_STI_THK_70000和PST_GLB_CMP_THK_80000计算失败: {e}")
            return df

# 注册映射器
MapperFactory.register("ColumnRenameMapper", ColumnRenameMapper)

