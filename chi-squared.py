from scipy.stats import chi2_contingency, chi2
import pandas as pd
import numpy as np

def compute_chi2(df, col1, col2, significance = 0.05):
  def counts_assign(m, i,  j, v):
    m[i, j] = v
  countsdf = df.groupby(by=[col1, col2]).size().reset_index(name='counts')
  col1Data = list(countsdf[col1].unique())
  col2Data = list(countsdf[col2].unique())
  counts = np.zeros((len(col1Data), len(col2Data)), dtype=np.int32)
  _ = countsdf.apply(lambda r: counts_assign(counts, col1Data.index(r[col1]), col2Data.index(r[col2]), r['counts']), axis=1)
  chi2_input_df = pd.DataFrame(counts, index=col1Data, columns=col2Data)
  chi, pval, dof, exp = chi2_contingency(chi2_input_df)
  p = 1 - significance
  critical_value = chi2.ppf(p, dof)
  return chi, pval, dof, exp, critical_value, chi2_input_df

tshirtsdata = pd.read_csv('gender-color.csv', sep=',')
chi, pval, dof, exp, critical_value, chi2_input_df = compute_chi2(tshirtsdata, 'gender', 'color')
