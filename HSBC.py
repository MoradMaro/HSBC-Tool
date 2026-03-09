
import streamlit as st
from config import init_page
init_page(title="HSBC")

from calendar import c
import pandas as pd
from PIL import Image
from Defs import *
from Defs import get_common_names_between_ATMs_and_DFF, convert_to_excel
from sqlalchemy import create_engine




#  إعدادات الصفحة
# st.set_page_config(page_title="HSBC", page_icon="🏦", layout="wide", initial_sidebar_state="auto")


#-------------------------------------------------------------------------------------------------------------------
# إخفاء الشريط الجانبي
# hide_streamlit_style = """
#     <style>
#     #MainMenu {visibility: hidden;}
#     footer {visibility: hidden;}
#     header {visibility: hidden;}
#     </style>
#     """
# st.markdown(hide_streamlit_style, unsafe_allow_html=True)

#-------------------------------------------------------------------------------------------------------------------
# اضافة صورة
image=Image.open(r"Images/Cash Management.jpg")
image = image.resize((6000, 900))
st.image(image)

#-------------------------------------------------------------------------------------------------------------------
# إضافة عنوان الصفحة

st.markdown("""
<h1 style='text-align: center;
            background: linear-gradient(to right, #FF0000, #8A2BE2, #00008B);
            -webkit-background-clip: text;
            color: transparent;
            font-weight: bold;'>
Cash Management System - HSBC Bank</h1>
""", unsafe_allow_html=True)



#------------------------------------------------------------------------------------------------------------------

# @st.cache_resource(ttl=600)
# def get_db_engine():
#     # معلومات الاتصال بقاعدة البيانات
#     DB_Host = "localhost"
#     DB_Name = "HSBC"
#     DB_User = "postgres"
#     DB_Password = "tawfik0120339601##"
#     DB_Port = "5432"
    
#     # إنشاء المحرك للاتصال بقاعدة البيانات
#     engine = create_engine(f'postgresql+psycopg2://{DB_User}:{DB_Password}@{DB_Host}:{DB_Port}/{DB_Name}')
#     return engine
#------------------------------------------------------------------------------------------------------------------
# @st.cache_data(ttl=600)
def get_data_from_table(table_name):
    engine = get_db_engine()
    with engine.connect() as conn:
        query = f'SELECT * FROM "{table_name}"'
        df = pd.read_sql(query, conn)
    return df

#------------------------------------------------------------------------------------------------------------------

# إظهار مؤشر التحميل
placeholder = st.empty()
with placeholder.container():
    st.write("Loading data, please wait...")


#------------------------------------------------------------------------------------------------------------------
# # افتراضاً أن لديك الدالة get_data_from_table لجلب البيانات
# ATMs = get_data_from_table("ATMs")
# Type_ATMs = get_data_from_table("DFF")

# placeholder.empty()
# # استخراج المناطق وعدد الـ ATMs
# Regions = ATMs['Center'].astype(str).str[:3].unique()
# Count_ATMs = ATMs['Center'].astype(str).str[:3].value_counts()

# # قائمة الدول المحددة
# valid_regions = ['UAE','EGY', 'BHR', 'QAT']

# # تصفية البيانات باستخدام شرط if للتحقق من وجود المنطقة في القائمة
# filtered_atms = {}
# for region, count in Count_ATMs.items():
#     if region in valid_regions:  # التحقق مما إذا كانت المنطقة ضمن الدول المحددة
#         filtered_atms[region] = count

# # ترتيب البيانات بعد التصفية
# sorted_ATMs = pd.Series(filtered_atms).sort_values(ascending=False)

# # عرض البيانات باستخدام الأعمدة بشكل ديناميكي
# st.subheader(f"HSBC Has ( 4 ) Regions : ") #( {', '.join(valid_regions)} )")

# # استخدم الأعمدة بشكل ديناميكي بناءً على عدد الدول
# columns = st.columns(len(valid_regions))

# # ربط كل منطقة بعدد الـ ATMs بشكل ديناميكي
# for i, region in enumerate(valid_regions):
#     atm_count = sorted_ATMs.get(region, 0)
#     with columns[i]:
#         st.markdown(f"""
#         <div style="
#             text-align: center;
#             line-height: 1.2;
#             margin-bottom: 2px;
#         ">
#             {region} : (<span style="color: green; font-size: 16px; "> {atm_count} </span> ATMs )
#         </div>
#         <hr style="border: 1px solid red; margin-top: 0px; margin-bottom: 8px;">
#         """, unsafe_allow_html=True)
#         st.write("Total ATMs : ", sorted_ATMs.sum())

# # عرض العدد الإجمالي للـ ATMs
# st.write("Total ATMs : ", sorted_ATMs.sum())

# #------------------------------------------------------------------------------------------------------------------

# جلب البيانات
ATMs = get_data_from_table("ATMs")

# استخراج المناطق وعدد الـ ATMs
Count_ATMs = ATMs['Center'].astype(str).str[:3].value_counts()

# قائمة المناطق المحددة
valid_regions = ['UAE','EGY', 'BHR', 'QAT']

# تصفية وترتيب المناطق المطلوبة فقط
sorted_ATMs = Count_ATMs.reindex(valid_regions).fillna(0).astype(int).sort_values(ascending=False)

# عرض العنوان
st.subheader(f"HSBC Has ({len(valid_regions)}) Regions:")

# إنشاء الأعمدة بناءً على عدد المناطق
columns = st.columns(len(valid_regions))

placeholder.empty()

# عرض كل منطقة وعدد الـ ATMs بشكل منسق
for i, region in enumerate(sorted_ATMs.index):
    atm_count = sorted_ATMs.loc[region]
    with columns[i]:
        st.markdown(f"""
        <div style="
            text-align: center;
            line-height: 1.2;
            margin-bottom: 4px;
            font-weight: bold;
        ">
            {region} : (<span style="
                color: #4caf50;  /* أخضر متناسق */
                font-size: 18px;
                padding: 2px 8px;
                background-color: rgba(76, 175, 80, 0.15);
                border-radius: 6px;
                box-shadow: 0 0 5px rgba(76, 175, 80, 0.2);
                font-family: monospace;
                ">
                {atm_count}
            </span> ATMs)
        </div>
        <hr style="border: 1px solid red; margin-top: 0px; margin-bottom: 8px;">
        """, unsafe_allow_html=True)

# عرض المجموع مرة واحدة أسفل الأعمدة
total_atms = sorted_ATMs.sum()
st.markdown(f"""
<div style="
    text-align: center;
    font-size: 20px;
    font-weight: bold;
    margin-top: 20px;
    color: #0f5132;
    background-color: rgba(15, 81, 50, 0.1);
    border: 2px solid #0f5132;
    padding: 8px 16px;
    box-shadow: 0 0 10px rgba(15, 81, 50, 0.2);
    border-radius: 8px;
    max-width: 300px;
    margin-left: auto;
    margin-right: auto;
">
    Total ATMs: {total_atms}
</div>
""", unsafe_allow_html=True)


