# create_illumifinifrs17_structure_fixed.py
import os

BASE = r"C:\Users\shany\OneDrive\Desktop\tho\taT\findw-curated\scripts\Query\erpdw"
SYSTEM = "illumifinifrs17"

def ensure(p): os.makedirs(p, exist_ok=True)
def touch(d, f):
    p = os.path.join(d, f)
    if not os.path.exists(p):
        open(p, "w", encoding="utf-8").close()

# ---------- Header ----------
hdr_root = os.path.join(BASE, SYSTEM, "general_ledger_header"); ensure(hdr_root)
hdr_controls = os.path.join(hdr_root, "Controls"); ensure(hdr_controls)
hdr_kc2 = os.path.join(hdr_controls, "KC2"); ensure(hdr_kc2)

# Files at header root (numbered)
files_header_root = [
    "01+lkp_actvty_gl_sbldgr_nm+rdm.sql",
    "02+lkp_actvty_ldgr_nm+rdm.sql",
    "03+lkp_jgLenty_cd+rdm.sql",                # adjust token if you use a different exact string
    "04+lkp_src_acct_to_glacct+rdm.sql",
    "05+source_df+sparksql.sql",
    "06+curated_error+sparksql+append+glue.sql",
    "07+curated_error+sparksql+overwrite+glue.sql",
    "08+finaldf+sparksql+overwrite.sql",
]
for f in files_header_root: touch(hdr_root, f)

# Files in Controls (outside KC2)
touch(hdr_controls, "general_ledger_header_count.sql")

# Files in Controls/KC2
touch(hdr_kc2, "general_ledger_header_kc2_controls.sql")

# ---------- Line Item ----------
li_root = os.path.join(BASE, SYSTEM, "general_ledger_line_item"); ensure(li_root)
li_controls = os.path.join(li_root, "Controls"); ensure(li_controls)
li_kc2 = os.path.join(li_controls, "KC2"); ensure(li_kc2)

# Files at line_item root (numbered)
files_line_root = [
    "01+lkp_actvty_gl_sbldgr_nm+rdm.sql",
    "02+lkp_actvty_ldgr_nm+rdm.sql",
    "03+lkp_src_ggrphy+rdm.sql",
    "04+dim_ggrphy+rdm.sql",
    "05+lkp_ifrs17_cntr_tag_vw+rdm.sql",
    "06+rr_ra01_cntr_to_cntr_mapping+rdm.sql",
    "07+rr_ra03_acct_to_cft_mapping+rdm.sql",
    "08+gl_source_data+sparksql.sql",
    "09+source_df+sparksql.sql",
    "10+valid_records+sparksql.sql",
    "11+curated_error+sparksql+append+glue.sql",
    "12+curated_error+sparksql+overwrite+glue.sql",
    "14+finaldf+sparksql+overwrite.sql",
]
for f in files_line_root: touch(li_root, f)

# Files in Controls (outside KC2)
touch(li_controls, "general_ledger_line_item_count.sql")
touch(li_controls, "general_ledger_line_item_sum.sql")

# Files in Controls/KC2
touch(li_kc2, "general_ledger_line_item_kc2_controls.sql")

print(f"Created structure under: {os.path.join(BASE, SYSTEM)}")
