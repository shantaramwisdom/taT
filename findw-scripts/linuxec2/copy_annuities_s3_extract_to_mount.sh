#!/bin/bash
########################################################################################################
####################################### DEVELOPMENT LOG ################################################
########################################################################################################
# DESCRIPTION - Script to do various housekeeping activities in the EC2 Machine
# 08/29/2022 - PRATHYUSH PREMACHANDRAN - Annuities TNPS Initial Install
# 08/05/2024 - SANGRAM PATIL - Extract file name changes for Neutral file, A&B, NBU and DSS
########################################################################################################
export AWS_SHARED_CREDENTIALS_FILE=/application/financedw/scripts/.aws/credentials
env=$1
db_name=$2
domain_name=$3
schema_name=$4
view_name=$5
source_system=$6
remove_header=$7
category=$8
others=''
others_arch=''
total_mandatory_args=$#
if [[ -z $remove_header ]]; then
	remove_header='N'
else
	if [[ ${remove_header^^} != 'Y' ]]; then
		remove_header='N'
	else
		remove_header='Y'
	fi
fi
if [[ -z $category ]]; then
	category=''
else
	total_mandatory_args=6
fi
if [ $total_mandatory_args != 6 ]; then
	echo "ERROR 10: Mandatory Parmeters Needed are 6 but received $total_mandatory_args. The order is environment_name, db_name, domain_name, schema_name, view_name and source sytem name. Optional Argument is Seventh One to Remove Header"
	exit 10
fi
if ! sh /application/financedw/scripts/utilities/remount.sh -e ${env} -m FINMOD; then
	echo "ERROR 12: Remount operation failed with exit code $?" >&2
	exit 12
fi
is_ctl='N'
if [ $source_system == 'asd' ]; then
	prefix='IS_00002_'
elif [ $source_system == 'newark' ]; then
	prefix='IS_00003_'
elif [ $source_system == 'sfindividual' ]; then
	prefix='IS_00006_'
elif [ $source_system == 'onedesktop' ] || [ $source_system == 'fcr' ]; then
	prefix='IS_00001_'
else
	prefix=''
	others='/neutral_files/temp'
	others_arch='/neutral_files'
	if [[ $view_name == "ctl_vw_${source_system}_${domain_name}" ]]; then
		is_ctl='Y'
	fi
fi
extract=/mnt/FINMOD/annuities/extracts${others}
archive=/mnt/FINMOD/annuities/extracts/archives${others_arch}
archive_file=${archive}
mkdir -p $extract
mkdir -p $archive
declare -A nbu_domains
nbu_domains=(['nbu_polcontract']='NBU_CONT_SRC_EXT' ['nbu_polfund']='NBU_FUND_SRC_EXT' ['nbu_polparticipant']='NBU_PARTI_SRC_EXT' ['nbu_trx_2gr']='NBU_GTRTX_SRC_EXT' ['nbu_polagent']='NBU_AGENT_SRC_EXT')
file_name_sfx=${prefix}${env}_findw_${source_system}_${domain_name}
extract_file_name_sfx=$extract/${prefix}${env}_findw_${source_system}_${domain_name}
file_name=${extract_file_name_sfx}_extract_$(date +%Y%m%d%H%M%S).csv
if [[ ! -z $others ]]; then
	#if ! sh /application/financedw/scripts/utilities/remount.sh -e ${env} -m VANTAGE; then
	#	echo "ERROR 15: Remount operation failed with exit code $?" >&2
	#	exit 15
	#fi
	file_name_sfx=BaNCS_${category^^}_${domain_name^^}
	if [[ "$domain_name" == 'nbu' ]]; then
		file_name_sfx=${nbu_domains[$domain_name]}
	fi
	if [[ "$view_name" =~ ^ctl ]]; then
		file_name_sfx=BaNCS_${category^^}_${domain_name^^}_CTL
		if [[ "$domain_name" == 'nbu' ]]; then
			file_name_sfx=${nbu_domains[$domain_name]}_CTL
		fi
	fi
fi
extract_file_name_sfx=$extract/${file_name_sfx}
file_name=${extract_file_name_sfx}.txt
if [[ $category == 'DSS' ]] || [[ $category == 'NPN' ]]; then
	txt_ext_domains=('commissionable_amount' 'all_agencyssn' 'all_agencyssn_with_business' 'tax_impact' 'agent_information' 'advance_retained_commission' 'policy_count_by_agency' 'policy_count_by_agent' 'commission_rate')
	curr_date=$(date +%Y%m%d)
	if echo "${txt_ext_domains[@]}" | grep -qw "$domain_name" || [[ $is_ctl == 'Y' ]]; then
		file_name=${extract_file_name_sfx}_${curr_date}.txt
	else
		file_name=${extract_file_name_sfx}_${curr_date}.csv
	fi
elif [[ $domain_name == 'aggregate' ]] || [[ $domain_name == 'gk_rmda' ]] || [[ $domain_name == 'rider_generation_override' ]] && [[ $is_ctl == 'N' ]]; then
	file_name=${extract_file_name_sfx}.csv
fi
echo "****************************************"
echo "Mount File Name is $file_name"
echo "Remove Header Flag is $remove_header"
echo "****************************************"
files=$(shopt -s nullglob dotglob; echo ${extract_file_name_sfx}*)
if (( ${#files} )); then
	if [[ "$files" == "$file_name" ]]; then
		created_on=$(stat -c %y $file_name)
		time_suffix=$(date -d "$created_on" "+%Y%m%d%H%M%S")
		archive_file=${archive}/${file_name_sfx}_${time_suffix}${file_name: -4}
		mv -f ${extract_file_name_sfx}* $archive_file
		RC=$?
		if [ ${RC} -eq 0 ]; then
			echo "Move Files ${extract_file_name_sfx}* to Archive File $archive_file is successful at $(date +%Y-%m-%d\ %H:%M:%S)"
		else
			echo "ERROR 20: Move Files ${extract_file_name_sfx}* to Archive File $archive_file has Failed with RC $RC at $(date +%Y-%m-%d\ %H:%M:%S)"
			exit 20
		fi
	else
		echo "Skipping Older Extract files Move to Archive as ${extract_file_name_sfx}* is empty (or does not exist)"
	fi
fi
find $archive -type f -mtime +7 -name "${file_name_sfx}*" -execdir rm - '{}' \;
RC=$?
if [ ${RC} -eq 0 ]; then
	echo "Deleting Older Files ${file_name_sfx}* created before Seven Days from $archive is successful at $(date +%Y-%m-%d\ %H:%M:%S)"
else
	echo "ERROR 30: Deleting Older Files ${file_name_sfx}* created before Seven Days from $archive has Failed with RC $RC at $(date +%Y-%m-%d\ %H:%M:%S)"
	exit 30
fi
s3_file_name=s3://ta-individual-findw-${env}-extracts/redshift/${db_name}/unload/${domain_name}/${schema_name}/${view_name}/${view_name}_000
s3_temp_file1=${s3_file_name}_bk1
s3_temp_file2=${s3_file_name}
s3_temp_file3=${s3_file_name}
RC2=0
RC3=0
if [[ $remove_header == 'Y' ]]; then
	echo "****************************************"
	echo "Removing Header from $s3_file_name"
	aws s3 cp $s3_file_name - | sed '1d' | aws s3 cp - $s3_temp_file1
	RC2=$?
	s3_temp_file2=${s3_temp_file1}
	s3_temp_file3=${s3_temp_file1}
	echo "****************************************"
fi
if [[ ! -z $others ]]; then
	echo "****************************************"
	echo "Removing \" from $s3_temp_file2"
	s3_temp_file2=${s3_file_name}_bk2
	aws s3 cp $s3_temp_file2 - | sed 's/\"//g' | aws s3 cp - $s3_temp_file3
	RC3=$?
	echo "****************************************"
fi
aws s3 cp $s3_temp_file3 $file_name --only-show-errors
RC=$?
if [[ $RC -eq 0 ]] && [[ $RC2 -eq 0 ]] && [[ $RC3 -eq 0 ]]; then
	echo "File Moved to Mount Successfully at $(date +%Y-%m-%d\ %H:%M:%S)"
	if [[ ! -z $others ]]; then
		filename=$(basename "$file_name")
		tmp_file_path="/tmp/$filename"
		echo "Copying $file_name to $tmp_file_path at $(date +%Y-%m-%d\ %H:%M:%S)"
		if ! cp "$file_name" "$tmp_file_path"; then
			echo "ERROR 40: Failed to copy file to temp location at $(date +%Y-%m-%d\ %H:%M:%S)"
			exit 40
		fi
		echo Copy Completed at $(date +%Y-%m-%d\ %H:%M:%S)
		file_len=$(cat "$tmp_file_path" | awk '{print length($0)}' | sort -g | uniq)
		unique_len=$(echo $file_len | grep -v '^$')
		if [[ ${remove_header} == 'Y' ]] && [[ ! -z $unique_len ]] && [[ $is_ctl == 'N' ]]; then
			echo "ERROR 50: File $file_name is having Varying Lengths of $unique_len. Failed at $(date +%Y-%m-%d\ %H:%M:%S)"
			rm -f "$tmp_file_path"
			exit 50
		fi
		echo Checking if Empty Lines in File at $(date +%Y-%m-%d\ %H:%M:%S)
		if grep -qFxF "" "$tmp_file_path"; then
			echo "ERROR 60: File $file_name contains Empty Lines. Failed at $(date +%Y-%m-%d\ %H:%M:%S)"
			rm -f "$tmp_file_path"
			exit 60
		fi
		echo "Converting Line Endings for $tmp_file_path at $(date +%Y-%m-%d\ %H:%M:%S)"
		if ! perl -pi.bak -e 's/\r\n/\n/g' "$tmp_file_path"; then
			echo "ERROR 70: Line ending conversion failed for $tmp_file_path at $(date +%Y-%m-%d\ %H:%M:%S)"
			sleep 2
			rm -f "$tmp_file_path" "${tmp_file_path}.bak"
			exit 70
		fi
		sleep 2
		rm -f "${tmp_file_path}.bak"
		echo "Line Endings Converted for $tmp_file_path at $(date +%Y-%m-%d\ %H:%M:%S)"
		if ! mv "$tmp_file_path" "$file_name"; then
			echo "ERROR 80: Failed to move converted file back at $(date +%Y-%m-%d\ %H:%M:%S)"
			rm -f "$tmp_file_path"
			exit 80
		fi
		echo "File $file_name moved back to original location at $(date +%Y-%m-%d\ %H:%M:%S)"
		created_on=$(stat -c %y $file_name)
		time_suffix=$(date -d "$created_on" "+%Y%m%d%H%M%S")
		archive_file=${archive}/${file_name_sfx}_${time_suffix}${file_name: -4}
		echo Archiving ${file_name} to $archive_file at $(date +%Y-%m-%d\ %H:%M:%S)
		if ! cp ${file_name} $archive_file; then
			echo "ERROR 90: Failed to archive file"
			exit 90
		fi
		echo Moving ${file_name} to /mnt/FINMOD/annuities/extracts/neutral_files/ at $(date +%Y-%m-%d\ %H:%M:%S)
		if ! mv ${file_name} /mnt/FINMOD/annuities/extracts/neutral_files/; then
			echo "ERROR 100: Failed to move file to neutral_files directory"
			exit 100
		fi
	else
		echo "ERROR 110: File Move to Mount Failed with Return Code $RC (Other Return Codes $RC2 and $RC3) at $(date +%Y-%m-%d\ %H:%M:%S)"
		exit 110
	fi
fi
echo "Script Completed Successfully at $(date +%Y-%m-%d\ %H:%M:%S)"
