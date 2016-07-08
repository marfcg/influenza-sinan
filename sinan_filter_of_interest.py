import pandas as pd
import argparse
from argparse import RawDescriptionHelpFormatter

# coding:utf8


def applysinanfilter(df):
    """Filter dataframe containing SINAN-SRAG data.
    Returns dataframe filtered by symptoms and containing notification week and date, discarding rows where any of
    those are missing. The returned dataframe contains only columns of interest from original data and extra
    standardized columns with lab test info.

    :param df: pandas.DataFrame object with SINAN-SRAG data, with columns from both pre and pos 2012 format
    :return df: pandas.DataFrame object with columns of interest only
    """
    tgtcols = ['SEM_NOT', 'DT_NOTIFIC', 'SG_UF_NOT', 'DT_INTERNA', 'DT_SIN_PRI', 'SRAG2012', 'DT_DIGITA',
               'FEBRE', 'TOSSE', 'GARGANTA', 'DISPNEIA', 'SATURACAO', 'DESC_RESP', 'EVOLUCAO',
               'DT_COLETA', 'IFI', 'DT_IFI', 'PCR', 'OUT_METODO', 'DS_OUTMET', 'DT_OUTMET', 'RES_FLUA',
               'RES_FLUASU', 'RES_FLUB', 'RES_VSR', 'RES_PARA1', 'RES_PARA2', 'RES_PARA3', 'RES_ADNO', 'RES_OUTRO',
               'DT_PCR', 'PCR_RES', 'PCR_ETIOL', 'PCR_TIPO_H', 'PCR_TIPO_N', 'DT_CULTURA', 'CULT_RES', 'DT_HEMAGLU',
               'HEMA_RES', 'HEMA_ETIOL', 'HEM_TIPO_H', 'HEM_TIPO_N', 'VACINA', 'DT_UT_DOSE', 'ANT_PNEUMO', 'DT_PNEUM',
               'CO_UF_INTE', 'CO_MU_INTE', 'CO_UN_INTE', 'DT_ENCERRA', 'NU_NOTIFIC', 'ID_AGRAVO', 'ID_MUNICIP',
               'ID_REGIONA', 'ID_UNIDADE', 'NU_IDADE_N', 'CS_SEXO', 'CS_GESTANT', 'CS_RACA', 'SG_UF', 'ID_MN_RESI',
               'ID_RG_RESI']
    df = df[tgtcols].copy()

    # Filter by notification date
    df.dropna(subset=['SEM_NOT', 'DT_NOTIFIC'], inplace=True)

    # Filter by symptoms
    df = df[((df.FEBRE == 1) &
             ((df.TOSSE == 1) | (df.GARGANTA == 1)) &
             ((df.DISPNEIA == 1) | (df.SATURACAO == 1) | (df.DESC_RESP == 1))) |
            (df.EVOLUCAO == 2)].copy()

    # Create columns related to lab result

    # Rows with lab tested cases:
    labrows = ((df.PCR_RES.isin([1, 2, 3])) | (df.CULT_RES.isin([1, 2])) | (df.HEMA_RES.isin([1, 2, 3])) |
               (df.IFI == 1) | (df.PCR == 1) | (df.OUT_METODO == 1))

    # Rows with untested cases:
    nottestedrows = ((df.PCR_RES.isin([4])) | (df.CULT_RES.isin([3])) | (df.HEMA_RES.isin([4])) | (pd.isnull(df.IFI) |
                                                                                                   df.IFI == 2) &
                     (pd.isnull(df.PCR) | df.PCR == 2) & (pd.isnull(df.OUT_METODO) | df.OUT_METODO == 2))
    df['NOT_TESTED'] = nottestedrows.astype(int)  # Case untested: 1-True, 2-False

    # Rows with unknown status regarding lab test
    notknownrows = ~(labrows | nottestedrows)
    df['TESTING_IGNORED'] = notknownrows.astype(int)  # Unknown whether case was tested or not: 1-True, 2-False

    df['FLU_A'] = None  # Flu A positive test result: 1-True, 2-False
    df['FLU_B'] = None  # Flu B positive test result: 1-True, 2-False
    df['VSR'] = None  # VSR positive test result: 1-True, 2-False
    df['OTHERS'] = None  # Other viruses positive test result: 1-True, 2-False
    df['NEGATIVE'] = None  # Negative test result: 1-True, 2-False
    df['INCONCLUSIVE'] = None  # Inconclusive test result: 1-True, 2-False
    df['DELAYED'] = None  # Unreported (i.e. delayed) test result: 1-True, 2-False

    df.loc[labrows, 'FLU_A'] = ((df.PCR_ETIOL[labrows].isin([1, 2, 4])) | (df.HEMA_ETIOL[labrows].isin([1, 2, 4])) |
                                (df.RES_FLUA[labrows] == 1)).astype(int)
    df.loc[labrows, 'FLU_B'] = ((df.PCR_ETIOL[labrows] == 3) | (df.HEMA_ETIOL[labrows] == 3) |
                                (df.RES_FLUB[labrows] == 1)).astype(int)
    df.loc[labrows, 'VSR'] = (df.RES_VSR[labrows] == 1).astype(int)
    df.loc[labrows, 'OTHERS'] = ((df.PCR_ETIOL[labrows] == 5) | (df.HEMA_ETIOL[labrows] == 5) |
                                 (df.RES_PARA1[labrows] == 1) | (df.RES_PARA2[labrows] == 1) |
                                 (df.RES_PARA3[labrows] == 1) | (df.RES_ADNO[labrows] == 1) |
                                 (df.RES_OUTRO[labrows] == 1)).astype(int)
    df.loc[labrows, 'DELAYED'] = ((pd.isnull(df.PCR_RES[labrows]) | df.PCR_RES[labrows] == 4) &
                                  (pd.isnull(df.HEMA_RES[labrows]) | df.HEMA_RES[labrows] == 4) &
                                  (pd.isnull(df.RES_FLUA[labrows]) | df.RES_FLUA[labrows] == 4) &
                                  (pd.isnull(df.RES_FLUB[labrows]) | df.RES_FLUB[labrows] == 4) &
                                  (pd.isnull(df.RES_VSR[labrows]) | df.RES_VSR[labrows] == 4) &
                                  (pd.isnull(df.RES_PARA1[labrows]) | df.RES_PARA1[labrows] == 4) &
                                  (pd.isnull(df.RES_PARA2[labrows]) | df.RES_PARA2[labrows] == 4) &
                                  (pd.isnull(df.RES_PARA3[labrows]) | df.RES_PARA3[labrows] == 4) &
                                  (pd.isnull(df.RES_ADNO[labrows]) | df.RES_ADNO[labrows] == 4) &
                                  (pd.isnull(df.RES_OUTRO[labrows]) | df.RES_OUTRO[labrows] == 4)).astype(int)
    df.loc[labrows, 'INCONCLUSIVE'] = ((df.DELAYED[labrows] == 0) &
                                       (pd.isnull(df.PCR_RES[labrows]) | df.PCR_RES[labrows].isin([3, 4])) &
                                       (pd.isnull(df.HEMA_RES[labrows]) | df.HEMA_RES[labrows].isin([3, 4])) &
                                       (pd.isnull(df.RES_FLUA[labrows]) | df.RES_FLUA[labrows].isin([3, 4])) &
                                       (pd.isnull(df.RES_FLUB[labrows]) | df.RES_FLUB[labrows].isin([3, 4])) &
                                       (pd.isnull(df.RES_VSR[labrows]) | df.RES_VSR[labrows].isin([3, 4])) &
                                       (pd.isnull(df.RES_PARA1[labrows]) | df.RES_PARA1[labrows].isin([3, 4])) &
                                       (pd.isnull(df.RES_PARA2[labrows]) | df.RES_PARA2[labrows].isin([3, 4])) &
                                       (pd.isnull(df.RES_PARA3[labrows]) | df.RES_PARA3[labrows].isin([3, 4])) &
                                       (pd.isnull(df.RES_ADNO[labrows]) | df.RES_ADNO[labrows].isin([3, 4])) &
                                       (pd.isnull(df.RES_OUTRO[labrows]) | df.RES_OUTRO[labrows].isin([3, 4]))).\
        astype(int)
    df.loc[labrows, 'NEGATIVE'] = ((df.FLU_A[labrows] == 0) & (df.FLU_B[labrows] == 0) & (df.VSR[labrows] == 0) &
                                   (df.OTHERS[labrows] == 0) & (df.DELAYED[labrows] == 0) &
                                   (df.INCONCLUSIVE[labrows] == 0)).astype(int)
    df['POSITIVE'] = df[['FLU_A', 'FLU_B', 'VSR', 'OTHERS']].sum(axis=1)

    return df


def sinanfilteredaggbyweek(df):
    """"Group by week"""

    # First off, fill NaN with 0 in order to preserve sum
    df_filled = df[['SG_UF_NOT', 'SEM_NOT', 'FLU_A', 'FLU_B', 'VSR', 'OTHERS', 'POSITIVE', 'NEGATIVE', 'INCONCLUSIVE',
                    'NOT_TESTED', 'TESTING_IGNORED']].fillna(0)

    df_filled['POSITIVE'] = df_filled[['FLU_A', 'FLU_B', 'VSR', 'OTHERS']].sum(axis=1)
    df_filled['TESTED'] = df_filled[['POSITIVE', 'NEGATIVE', 'INCONCLUSIVE']].sum(axis=1)
    df_filled['TOTAL_CASES'] = df_filled[['TESTED', 'NOT_TESTED', 'TESTING_IGNORED']].sum(axis=1)

    dfweekly = df_filled.groupby(['SG_UF_NOT', 'SEM_NOT'], as_index=False).agg(sum)

    return dfweekly


def main(flist, sep=',', uf=None):
    print('File list: %s' % (', '.join(flist)))
    print('Reading file: %s' % flist[0])
    df = pd.read_csv(flist[0], sep=sep)

    # If there are empty values in column SG_UF_NOT, this column will be treated as float by default.
    # This conversion to float will add '.0' to all non-empty values when converting back to string and/or writing.
    # Force column to be string, removing floating point from non-empty values:
    df.SG_UF_NOT = df.SG_UF_NOT.apply(lambda x: str(x).replace('.0', ''))
    if uf is not None:
        df = df[df.SG_UF_NOT.astype(str) == uf].copy()

    dfclean = applysinanfilter(df)
    for fname in flist[1:]:
        print('Reading file: %s' % fname)
        df = pd.read_csv(fname, sep=sep)
        df.SG_UF_NOT = df.SG_UF_NOT.apply(lambda x: str(x).replace('.0', ''))
        if uf is not None:
            df = df[df.SG_UF_NOT.astype(str) == uf].copy()

        dfclean = dfclean.append(applysinanfilter(df), ignore_index=True)

    # Aggregate total cases and lab results by notification week:
    dfcleanweekly = sinanfilteredaggbyweek(dfclean)

    dfclean.to_csv('clean_data_sinan_filter_of_interest.csv', index=False)
    dfcleanweekly.to_csv('clean_data_sinan_filter_of_interest_weekly.csv', index=False)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Clean SINAN SRAG table.\n' +
                                                 'python3 sinan_filter_of_interest.py --path data/influ*.csv --sep , '
                                                 '--UF 35\n',
                                     formatter_class=RawDescriptionHelpFormatter)
    parser.add_argument('--path', nargs='*', action='append', help='Path to data file')
    parser.add_argument('--sep', help='Column separator', default=',')
    parser.add_argument('--UF', help='Tgt_UF_GeoCode', default=None)
    args = parser.parse_args()
    print(args)
    main(args.path[0], args.sep, args.UF)
