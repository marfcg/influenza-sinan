# Apply filters of interest in a dataframe containing SINAN-SRAG data
import pandas as pd


def applysinanfilter(df):

    # Filter columns of interest
    tgtcols = ['SEM_NOT', 'DT_NOTIFIC', 'SG_UF_NOT', 'DT_INTERNA', 'DT_SIN_PRI', 'SRAG2012', 'DT_DIGITA',
               'FEBRE', 'TOSSE', 'GARGANTA', 'DISPNEIA', 'SATURACAO', 'DESC_RESP', 'EVOLUCAO',
               'DT_COLETA', 'IFI', 'DT_IFI', 'PCR', 'OUT_METODO', 'DS_OUTMET', 'DT_OUTMET', 'RES_FLUA',
               'RES_FLUASU', 'RES_FLUB', 'RES_VSR', 'RES_PARA1', 'RES_PARA2', 'RES_PARA3', 'RES_ADNO', 'RES_OUTRO',
               'DT_PCR', 'PCR_RES', 'PCR_ETIOL', 'PCR_TIPO_H', 'PCR_TIPO_N', 'DT_CULTURA', 'CULT_RES', 'DT_HEMAGLU',
               'HEMA_RES', 'HEMA_ETIOL', 'HEM_TIPO_H', 'HEM_TIPO_N','VACINA','DT_UT_DOSE','ANT_PNEUMO','DT_PNEUM',
               'CO_UF_INTE','CO_MU_INTE','CO_UN_INTE','DT_ENCERRA','NU_NOTIFIC','ID_AGRAVO','ID_MUNICIP',
               'ID_REGIONA', 'ID_UNIDADE','NU_IDADE_N','CS_SEXO','CS_GESTANT','CS_RACA','SG_UF','ID_MN_RESI',
               'ID_RG_RESI']
    df = df[tgtcols].copy()

    # Filter by notification date
    df.dropna(subset=["SEM_NOT", "DT_NOTIFIC"], inplace=True)


    # Filter by symptoms
    df = df[((df.FEBRE == 1) &
         ((df.TOSSE == 1) | (df.GARGANTA == 1)) &
         ((df.DISPNEIA == 1) | (df.SATURACAO == 1) | (df.DESC_RESP == 1))) |
        (df.EVOLUCAO == 2)].copy()


    # Create columns related to lab result
    
    # Rows with lab test:
    labrows = ( (df.PCR_RES.isin([1, 2, 3])) |
                (df.CULT_RES.isin([1, 2])) |
                (df.HEMA_RES.isin([1, 2, 3])) |
                (df.IFI == 1) |
                (df.PCR == 1) |
                (df.OUT_METODO == 1) )

    df['FLU_A'] = None
    df['FLU_B'] = None
    df['VSR'] = None
    df['OTHERS'] = None
    df['NEGATIVE'] = None
    df['INCONCLUSIVE'] = None
    df['DELAYED'] = None

    df['NOTTESTED'] = (~labrows).astype(int)
    df.loc[labrows, 'FLU_A'] = ((df.PCR_ETIOL[labrows].isin([1,2,4])) | (df.HEMA_ETIOL[labrows].isin([1,2,4]) |
                         (df.RES_FLUA[labrows] == 1)).astype(int)
    df.loc[labrows, 'FLU_B'] = ((df.PCR_ETIOL[labrows] == 3) | (df.HEMA_ETIOL[labrows] == 3) |
                         (df.RES_FLUB[labrows] == 1)).astype(int)
    df.loc[labrows, 'VSR'] = (df.RES_VSR[labrows] == 1).astype(int)
    df.loc[labrows, 'OTHERS'] = ((df.PCR_ETIOL[labrows] == 5) | (df.HEMA_ETIOL[labrows] == 5) | (df.RES_PARA1[labrows] == 1) |
                          (df.RES_PARA2[labrows] == 1) | (df.RES_PARA3[labrows] == 1) | (df.RES_ADNO[labrows] == 1) |
                          (df.RES_OUTRO[labrows] == 1)).astype(int)
    df.loc[labrows, 'DELAYED'] = ( (pd.isnull(df.PCR_RES[labrows]) | df.PCR_RES == 4) &
                            (pd.isnull(df.HEMA_RES[labrows]) | df.HEMA_RES == 4) &
                            (pd.isnull(df.RES_FLUA[labrows]) | df.RES_FLUA[labrows] == 4) &
                            (pd.isnull(df.RES_FLUB[labrows]) | df.RES_FLUB[labrows] == 4) &
                            (pd.isnull(df.RES_VSR[labrows]) | df.RES_VSR[labrows]== 4) &
                            (pd.isnull(df.RES_PARA1[labrows]) | df.RES_PARA1[labrows] == 4) &
                            (pd.isnull(df.RES_PARA2[labrows]) | df.RES_PARA2[labrows] == 4) &
                            (pd.isnull(df.RES_PARA3[labrows]) | df.RES_PARA3[labrows] == 4) &
                            (pd.isnull(df.RES_ADNO[labrows]) | df.RES_ADNO[labrows] == 4) &
                            (pd.isnull(df.RES_OUTRO[labrows]) | df.RES_OUTRO[labrows] == 4) ).astype(int)
    df.loc[labrows, 'INCONCLUSIVE'] = ( df.DELAYED[labrows] == 0  &
                                 (pd.isnull(df.PCR_RES[labrows]) | df.PCR_RES.isin([3,4])) &
                                (pd.isnull(df.HEMA_RES[labrows]) | df.HEMA_RES.isin([3,4])) &
                                (pd.isnull(df.RES_FLUA[labrows]) | df.RES_FLUA[labrows].isin([3,4])) &
                                (pd.isnull(df.RES_FLUB[labrows]) | df.RES_FLUB[labrows].isin([3,4])) &
                                (pd.isnull(df.RES_VSR[labrows]) | df.RES_VSR[labrows].isin([3,4])) &
                                (pd.isnull(df.RES_PARA1[labrows]) | df.RES_PARA1[labrows].isin([3,4])) &
                                (pd.isnull(df.RES_PARA2[labrows]) | df.RES_PARA2[labrows].isin([3,4])) &
                                (pd.isnull(df.RES_PARA3[labrows]) | df.RES_PARA3[labrows].isin([3,4])) &
                                (pd.isnull(df.RES_ADNO[labrows]) | df.RES_ADNO[labrows].isin([3,4])) &
                                (pd.isnull(df.RES_OUTRO[labrows]) | df.RES_OUTRO[labrows].isin([3,4]))).astype(int)
    df.loc[labrows, 'NEGATIVE'] = ((df.FLU_A[labrows] == 0) & (df.FLU_B[labrows] == 0) & (df.VSR[labrows] == 0) &
                            (df.OTHERS[labrows] == 0) & (df.DELAYED[labrows] == 0) &
                            (df.INCONCLUSIVE[labrows] == 0)).astype(int)

    return(df)



