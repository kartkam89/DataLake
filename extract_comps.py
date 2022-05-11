from sklearn.feature_extraction.text import CountVectorizer as cv
from sklearn.feature_extraction.text import TfidfVectorizer as tf
import pandas as pd


def extract_comps():

    comp = set(['451 research',
                'ponemon',
                'kuppingercole',
                'forbes',
                'forrester',
                'isg provider lens',
                'frost & sullivan',
                'idc',
                'esg',
                'aberdeen',
                'ibm corporation',
                'gartner',
                'nucleus',
                'bloor',
                'sans',
                'ibm institute for business value',
                'evaluator group',
                'enterprise management associates',
                'idg',
                'hfs research',
                'techtarget',
                'barc',
                'cabot partners group',
                'futurum',
                'ihl group',
                'solitaire',
                'appledore research',
                'celent',
                'ventana',
                'quark lepton',
                'hurwitz',
                'technology business research',
                'verdantix',
                'clabby analytics',
                'ovum ict',
                'incisiv',
                'chartis',
                'it central station',
                'tirias research',
                'oxford economics',
                "o'reilly",
                'robert frances group',
                'pund-it',
                'ziff davis',
                'g2',
                'everest',
                'javelin',
                'itic',
                'intelligent solutions',
                'principled',
                'techotherclarity',
                'rtinsights',
                'acoustics',
                'intelligent business strategies',
                'nelsonhall',
                'siverton consulting',
                'creative',
                'sdx central',
                'biz tech insights',
                'edison group',
                'hardenstance',
                'isg insights',
                'cefro'])

    partner_text = ['commissioned', 'sponsored', 'in association with', 'in partnership with',
                    'report for', 'commissionato da', 'auftrag von', 'excerpt for', 'in zusammenarbeit mit',
                    'encargado por ', 'commandée par ', 'sponsor', 'solicitado por ', 'sponsorisé par',
                    'patrocinado por']





    df = pd.read_excel("c:\\garbage\\AR\\output.xlsx",sheet_name="Sheet1")
    print(df.columns)

    if len(df) > 0:
        df['full_text'] = df['first page text']	+ df['last page text'] + df['asset name'] + df['asset url']

        for i in comp:
            df[i] = list(map(lambda x: x.lower().count(i),df['full_text'].astype("str")))

        print(df.iloc[:,9:])
        df['comp_check_val'] = list(df.iloc[:,9:].sum(axis=1))
        df['comp_hat'] = list(df[df.columns[9:72]].idxmax(axis=1))
        df['comp_hat'] = list(map(lambda x,y: x if y>0 else "inconclusive",df['comp_hat'],df['comp_check_val']))
        comm = []
        for i in df['full_text']:
            x = [1 for j in partner_text if str(j).lower() in str(i).lower()]
            if sum(x) > 1:
                comm.append("Yes")
            else:
                comm.append("No")
        df['comm'] = comm

    df.to_excel("c:\\garbage\\firms_check.xlsx")
    return df[['asset_code','asset name', 'asset location','comp_hat','comm']]



    # tfvect = tf()
    # tra = tfvect.fit_transform([" ".join(comp)])
    # op = tfvect.transform(df['full_text']).toarray()
    # dft = pd.DataFrame(op)
    #
    # fields = []
    # for i in sorted(tfvect.vocabulary_.values(),reverse=False):
    #     fields.append(list(tfvect.vocabulary_.keys())[list(tfvect.vocabulary_.values()).index(i)])
    #
    # dft.columns = fields
    # dft.to_excel("c:\\garbage\\appler.xlsx")

if __name__ == "__main__":
    extract_comps()