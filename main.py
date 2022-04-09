from cgitb import text
from distutils import text_file
from flask import Flask, render_template, request, redirect, url_for,send_file
# 2 csv 
#dc kitchen
import pandas as pd
import copy
import numpy as np
app = Flask(__name__)

@app.route('/', methods=['GET', 'POST'])
def index():
    return render_template('index.html')

@app.route('/data', methods=['GET', 'POST'])
def data():
    if request.method == 'POST':
        file=request.form['upload_file1']# lst of column names which needs to be string
        file2=request.form['upload_file2']# lst of column names which needs to be string
        # lst of column names which needs to be string
        lst_str_cols = ['DcNumber','Kitchen_id','cafecode','Transaction_date']
        lst_str_cols1 = ['Trans Date']
        # use dictionary comprehension to make dict of dtypes
        dict_dtypes = {x : 'str'  for x in lst_str_cols}
        dict_dtypes1 = {y : 'str'  for y in lst_str_cols1}
        df = pd.read_excel(file2,dtype=dict_dtypes)
        df2 = pd.read_excel(file,dtype=dict_dtypes1)

        det_df = pd.DataFrame()

        # cleaning modified dc report
            
        raw_modified =df2.iloc[8:]

        raw_modified.apply(lambda x: pd.Series(x.dropna().values))

        header_row = 0
        raw_modified = raw_modified.reset_index(drop=True)
        raw_modified = raw_modified.drop(header_row)
        raw_modified
        #primary df to temp df
        deep = raw_modified.copy()
        deep = deep.iloc[: , :]
        deep.tail()
        #raw_modified.tail()
        r_mf=deep.dropna(axis=1)
        r_mf1 = pd.concat([r_mf, raw_modified['Unnamed: 18']], axis=1)

        dictd = {'COFFEE DAY GLOBAL LIMITED':'Kitchen ID','Unnamed: 4':'Outlet Id','Unnamed: 6':'Trans Date','Unnamed: 7':'DC NO','Unnamed: 9':'Item Code','Unnamed: 10':'Qty','Unnamed: 12':'Cost Price','Unnamed: 13':'Value','Unnamed: 14':'Std Cost Price','Unnamed: 16':'Std Cost','Unnamed: 17':'Unit','Unnamed: 18':'Indent No'}
        # call rename () method
        r_mf1.rename(columns=dictd,inplace=True)
        print(r_mf1)


        # In[126]:


        # dowload xls dc from ax = r_mf1
        # yesterday generated dc = df

        df_Target = pd.DataFrame(df['DcNumber'])
        df_Target["n/a"] = ""
        dc_v = df_Target.copy()
        dc_v.set_index('DcNumber',inplace= True)
        dc_v["n/a"] = dc_v.index.map(r_mf1["DC NO"])
        dc_v1 = dc_v[dc_v['n/a'].notna()]
        #--------------------------------------- not working--------------------------------


        # In[127]:


        r_mf1.head()


        # In[128]:


        df.head()


        # In[129]:


        #comapring dc no vlookup InDf2 must be 0 (if 1 same dc no exists )
        r_mf1=r_mf1.assign(InDf2=r_mf1["DC NO"].isin(df.DcNumber).astype(int))
        df_clean_dc = r_mf1[r_mf1.InDf2 != 1]
        df_clean_dc.head()


        # In[130]:


        #importing site master 
        lst_str_cols3 = ['Site','Type','Old Name','New Name','State code','State name','Site GSTIN','Supplier id']
        # use dictionary comprehension to make dict of dtypes
        dict_dtypes3 = {x : 'str'  for x in lst_str_cols3}
        sitemaster = pd.read_excel("SiteMaster.xlsx",dtype=dict_dtypes3)
        sitemaster.head()


        # In[132]:



        #comparing outlet id vlookup 
        process_1_dc=df_clean_dc.copy()
        process_1_dc=process_1_dc.assign(InDf3=process_1_dc["Outlet Id"].isin(sitemaster.Site).astype(int))
        process_1_dc = process_1_dc[process_1_dc.InDf3 != 1]
        df_clean_dc1=process_1_dc.copy()
        df_clean_dc1.head()


        # In[133]:


        #splitting indent
        df_clean_dc1['clean_indent'] = df_clean_dc1['Indent No'].str.split('_').str[1]
        df_clean_dc1.head()
        df_clean_dc1 = df_clean_dc1.iloc[1: , :]
        dictd2 = {'Unnamed: 1':'Kitchen ID'}
        df_clean_dc1.rename(columns=dictd2,inplace=True)
        # In[134]:


        #supp supply check in site master 
        #up date bellow csv if any changes


        # In[135]:

        
        supp_py = pd.read_csv("supp_line_main_for python.csv",converters={'Site': lambda x: str(x)})
        area_dict=pd.Series(supp_py.Supplier_id.values,index=supp_py.Site).to_dict()
        df_clean_dc1["supp"] = df_clean_dc1["Kitchen ID"].apply(lambda x: area_dict.get(x))
        df_clean_dc1.head()


        # In[136]:


        duplicate = df_clean_dc1[df_clean_dc1.duplicated(['Kitchen ID','Outlet Id','Trans Date','DC NO','Item Code'],keep=False)]
        #with pd.option_context('display.max_rows', None, 'display.max_columns', None):  # more options can be specified also
       #     print(duplicate)


        # In[137]:


        #delete all duplicate rows 
        df_clean_dc1 = df_clean_dc1.drop_duplicates(subset=['Kitchen ID','Outlet Id','Trans Date','DC NO','Item Code'], keep=False)
       # with pd.option_context('display.max_rows', None, 'display.max_columns', None):  # more options can be specified also
        #    print(df_clean_dc1)


        # In[138]:


        #duplicate.groupby(['DC NO','Item Code'])['Qty'].sum()
        #,'Cost Price','Value','Std Cost Price','Std Cost','Unit','Indent No','InDf2','InDf3','clean_indent','supp'
        #df_clean_dc1.count()


        # In[139]:


        f = {'Qty':'sum','Cost Price': 'first','Value': 'first','Std Cost Price': 'first','Std Cost': 'first','Unit': 'first','Indent No': 'first','InDf2': 'first','InDf3': 'first','clean_indent': 'first','supp': 'first'}
        gn = duplicate.groupby(['Kitchen ID','Outlet Id','Trans Date','DC NO','Item Code'], as_index=False).agg(f)
        gn['Value']=gn['Qty']*gn['Cost Price']
        gn


        # In[140]:


        #concating check value to Df_clean
        frames= [df_clean_dc1,gn]
        df_clean_dc11=pd.concat(frames)


        # In[141]:


        #for pivot operation
        t = {'Item Code':'count'}
        povt_t=df_clean_dc11.groupby(['DC NO'], as_index=False).agg(t)
        povt_t.head()


        # In[142]:


        inner_join = pd.merge(df_clean_dc11, povt_t, on ='DC NO', how ='inner')
        inner_join


        # In[143]:


        DC_format_to_upload_pos= inner_join[['Kitchen ID', 'Outlet Id','supp','DC NO','Trans Date','clean_indent','Item Code_y','Item Code_x','Unit','Qty','Cost Price','Value']].copy()

        DC_format_to_upload_pos.reset_index(inplace = True, drop = True)
        DC_format_to_upload_pos.index = np.arange(1, len(DC_format_to_upload_pos) + 1)
        DC_format_to_upload_pos.Qty = np.array(DC_format_to_upload_pos.Qty, dtype=np.float)
        DC_format_to_upload_pos.Qty=DC_format_to_upload_pos.Qty.round(2)
        DC_format_to_upload_pos.Value = np.array(DC_format_to_upload_pos.Value, dtype=np.float)
        DC_format_to_upload_pos.Value=DC_format_to_upload_pos.Value.round(2)
        DC_format_to_upload_pos['Cost Price'] = np.array(DC_format_to_upload_pos['Cost Price'], dtype=np.float)
        DC_format_to_upload_pos['Cost Price']=DC_format_to_upload_pos['Cost Price'].round(2)
        import datetime as dt
        DC_format_to_upload_pos['Trans Date']= pd.to_datetime(DC_format_to_upload_pos['Trans Date'], errors='coerce')
        DC_format_to_upload_pos['Trans Date'] = DC_format_to_upload_pos['Trans Date'].dt.strftime("%d-%b-%Y")
        DC_format_to_upload_pos


        # In[144]:

        tempo= {'Kitchen ID':'Kitchen_id','Outlet Id':'cafecode','supp':'Suplier_id','DC NO':'DcNumber','Trans Date':'Transaction_date','clean_indent':'Indent number','Item Code_y':'Line_itemcount','Item Code_x':'Itemcode','Unit':'Unit_code','Qty':'item_qty','Cost Price':'cost_price','Value':'amount'}
        DC_format_to_upload_pos.rename(columns=tempo,inplace=True)


        # In[145]:

        from datetime import datetime
        text_doc = datetime.now().strftime("DC_format_text_%d-%m-%Y-hr%H-min%M-sec%S.txt")
        xlsx_file = datetime.now().strftime("DC_format_%d-%m-%Y-hr%H-min%M-sec%S.xlsx")
        import zipfile
        zipo=datetime.now().strftime("DC_format_zip_%d-%m-%Y-hr%H-min%M-sec%S.zip")
        
        DC_format_to_upload_pos.to_excel(xlsx_file)
        DC_format_to_upload_pos.to_csv(text_doc, header=None, sep=',',mode='a',decimal='.')
        list_files = [text_doc,xlsx_file]

        with zipfile.ZipFile(zipo, 'w') as zipF:
            for file1 in list_files:
                zipF.write(file1, compress_type=zipfile.ZIP_DEFLATED)
        #print(type(cd))
        print(type(zipo))
        return send_file(zipo,as_attachment=True)                
        #return render_template('data.html',data=data)

if __name__ == '__main__':
    app.run()
