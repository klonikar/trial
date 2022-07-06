from scipy.stats import chi2_contigency, chi2
import pandas pd
import numpy as np

tshirtdata = pd.read_csv('gender-color.csv', sep=',')
tshirtscountsdf = tshirtdata.groupby(by=['gender', 'color']).size().reset_index(name='counts')
genders = list(tshirtscountsdf['gender'].unique())
colors = list(tshirtscountsdf['color'].unique())
tshirt_counts = np.zeros((len(genders), len(colors)), dtype=np.int32)

def counts_assign(m, i,  j, v):
  m[i, j] = v

_ = tshirtscountsdf.apply(lambda r: counts_assign(tshirt_counts, genders.index(r['gender']), colors.index(r['color']), r['counts']), axis=1)
tshirts_df = pd.DataFrame(tshirt_counts, index=genders, columns=colors)
chi, pval, dof, exp = chi2_contigency(tshirts_df)
significance = 0.05
p = 1 - significance
critical_value = chi2.ppf(p, dof)
