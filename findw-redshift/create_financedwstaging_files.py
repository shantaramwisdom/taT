import os
# import os for filesystem operations
base = r'C:\Users\shany\OneDrive\Desktop\findw-redshift'
# base folder where folders/files will be created (no trailing backslash)
folders_and_files = {
# mapping: folder path (relative to base) -> list of filenames
"configs/archive": [
# files that appear under configs/archive in the screenshots
"deploy_1.3.config","deploy_1.5.config","deploy_2.0.config","deploy_2.1.config","deploy_2.2.config","deploy_2.3.config","deploy_2.4.5.config","deploy_3.0.config","deploy_3.1.config","deploy_3.2.config","deploy_3.3.6_1.config","deploy_3.3.6_2.config","deploy_aah.config","deploy_alfa_decom.config","deploy_all_bancs_bkup.config","deploy_bancs.config","deploy_bancs_access.config","deploy_bancs_alter.config","deploy_bancs_bkup.config","deploy_bancs_initial_load.config","deploy_bancs_model.config","deploy_bancs_pending.config","deploy_bancs_views.config","deploy_batchid_changes.config","deploy_bau.config","deploy_curated.config","deploy_curated_ltcg_mdl.config","deploy_curated_temp.config","deploy_domain_tracker.config","deploy_domain_tracker_changes.config","deploy_findw_erp_systems.config","deploy_findw_erp_systems_release.config","deploy_fund.config","deploy_gl.config","deploy_glr2.config","deploy_glv1.config","deploy_GLR1.3.config","deploy_GLR2.config","deploy_hotfix_INC1154406.config","deploy_hotfix_INC1160549.config","deploy_JNC1391667.config","deploy_mds_alm.config","deploy_mds_p5.config","deploy_mds_p6.config","deploy_mds_p85.config","deploy_mds_p75.config","deploy_mds_rest.config","deploy_mds_spl.config","deploy_nov_2023_release.config","deploy_prod_12012021.config","deploy_rebuild_controls.config","deploy_remove_rdm_view.config","deploy_sweepjob.config","expand_domain_name.config","kc2_rebuild.config","README.md"
# end of configs/archive file list
],
"configs": [
# additional deploy files visible in the same configs area (placed under configs/)
"deploy_all.config","deploy_bestow_backup.config","deploy_bestow_restore.config","deploy_create_schemas.config","deploy_custom.config","deploy_data.config","deploy_dba_actions.config","deploy_drop_all_schemas.config","deploy_drop_external_schemas.config","deploy_drop_internal_schemas.config","deploy_erp_all.config","deploy_erp_arr.config","deploy_erp_jcos.config","deploy_erp_revmap.config","deploy_erp_temp.config","deploy_erpdw_golden.config","deploy_erpdw_model_diff.config","deploy_erpdw_prod.config","deploy_GL_R3.config","deploy_GLR2.1.config","deploy_materialized_views.config","deploy_mkdb.config","deploy_procedures.config"
# end of configs file list
]
}
# dictionary prepared from the screenshots (best-effort transcription)
for folder, files in folders_and_files.items():
# iterate each folder entry
  dirpath = os.path.join(base, folder)
# build full directory path under base
  os.makedirs(dirpath, exist_ok=True)
# create directory (including parents) if missing
  for fname in files:
# iterate filenames for this folder
    full = os.path.join(dirpath, fname)
# build full path to the file to create
    if not os.path.exists(full):
# only create the file if it does not already exist
      open(full, "w", encoding="utf-8").close()
# create an empty file with utf-8 encoding
    print("Created or exists:", full)
# print the path created or already existing
