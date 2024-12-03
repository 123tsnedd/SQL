


WITH combined_data AS (
  --COMBINE ALL SOURCES
  SELECT 
  	TO_CHAR(ts, 'YYYY-MM-DD HH24:MI:SS') AS ts_utc,
  	msg ->> 'message' AS comments_changes,
  	NULL AS obsname,
  	NULL AS observer,
 		NULL AS holoop_state,
    NULL AS dimm_fwhm_corr,
    NULL AS fwsci1,
    NULL AS camsci1_exptime,
    NULL AS camsci1_emgain,
    NULL AS camsci1_read_out_speed,
    NULL AS camsci1_shutter_state,
    NULL AS camsci1_roi,
    NULL AS fwsci2,
    NULL AS camsci2_exptime,
    NULL AS camsci2_emgain,
    NULL AS camsci2_read_out_speed,
    NULL AS camsci2_shutter_state,
    NULL AS camsci2_roi,
    NULL AS camwfs_exptime,
    NULL AS camwfs_gain,
    NULL AS flipacq,
    NULL AS stagebs,
    NULL AS fwpupil,
    NULL AS fwfpm,
    NULL AS fwlyot,
    NULL AS stagescibs,
    NULL AS flipwfsf
  FROM user_log_partition
  WHERE ts BETWEEN '2024-11-18' AND '2024-11-20'
  
UNION ALL
  
  SELECT
  	TO_CHAR(ts, 'YYYY-MM-DD HH24:MI:SS') AS ts_utc,
  	NULL AS comments_changes,
  	msg ->> 'obsName' AS obsname,
  	msg ->> 'observer' AS observer,
  	NULL AS holoop_state,
    NULL AS dimm_fwhm_corr,
    NULL AS fwsci1,
    NULL AS camsci1_exptime,
    NULL AS camsci1_emgain,
    NULL AS camsci1_read_out_speed,
    NULL AS camsci1_shutter_state,
    NULL AS camsci1_roi,
    NULL AS fwsci2,
    NULL AS camsci2_exptime,
    NULL AS camsci2_emgain,
    NULL AS camsci2_read_out_speed,
    NULL AS camsci2_shutter_state,
    NULL AS camsci2_roi,
    NULL AS camwfs_exptime,
    NULL AS camwfs_gain,
    NULL AS flipacq,
    NULL AS stagebs,
    NULL AS fwpupil,
    NULL AS fwfpm,
    NULL AS fwlyot,
    NULL AS stagescibs,
    NULL AS flipwfsf
  FROM
  	active_observing_partition
  WHERE
  	ts BETWEEN '2024-11-18' AND '2024-11-20'
 
UNION ALL
  
  SELECT
  	TO_CHAR(ts, 'YYYY-MM-DD HH24:MI:SS') AS ts_utc,
  	NULL AS comments_changes,
  	NULL AS obsname,
  	NULL AS observer,
  	CASE WHEN device = 'holoop' THEN msg ->> 'state' END AS holoop_state,
    CASE WHEN device = 'tcsi' AND ec = 'telem_telsee' THEN msg ->> 'dimm_fwhm_corr' END AS dimm_fwhm_corr,
    CASE WHEN device = 'fwsci1' THEN msg ->> 'presetName' END AS fwsci1,
    CASE WHEN device = 'camsci1' THEN msg ->> 'exptime' END AS camsci1_exptime,
    CASE WHEN device = 'camsci1' THEN msg ->> 'emGain' END AS camsci1_emgain,
    CASE WHEN device = 'camsci1' THEN msg ->> 'adcSpeed' END AS camsci1_read_out_speed,
    CASE WHEN device = 'camsci1' THEN (msg -> 'shutter' ->> 'state') END AS camsci1_shutter_state,
    CASE WHEN device = 'camsci1' THEN (msg -> 'roi' ->> 'h') END || 'x' ||
        CASE WHEN device = 'camsci1' THEN (msg -> 'roi' ->> 'w') END AS camsci1_roi,
    CASE WHEN device = 'fwsci2' THEN msg ->> 'presetName' END AS fwsci2,
    CASE WHEN device = 'camsci2' AND ec = 'telem_stdcam' THEN msg ->> 'exptime' END AS camsci2_exptime,
    CASE WHEN device = 'camsci2' AND ec = 'telem_stdcam' THEN msg ->> 'emGain' END AS camsci2_emgain,
    CASE WHEN device = 'camsci2' AND ec = 'telem_stdcam' THEN msg ->> 'adcSpeed' END AS camsci2_read_out_speed,
    CASE WHEN device = 'camsci2' AND ec = 'telem_stdcam' THEN (msg -> 'shutter' ->> 'state') END AS camsci2_shutter_state,
    CASE WHEN device = 'camsci2' AND ec = 'telem_stdcam' THEN (msg -> 'roi' ->> 'h') END || 'x' ||
        CASE WHEN device = 'camsci2' AND ec = 'telem_stdcam' THEN (msg -> 'roi' ->> 'w') END AS camsci2_roi,
    CASE WHEN device = 'camwfs' AND ec = 'telem_stdcam' THEN msg ->> 'exptime' END AS camwfs_exptime,
    CASE WHEN device = 'camwfs' AND ec = 'telem_stdcam' THEN msg ->> 'emGain' END AS camwfs_gain,
    CASE WHEN device = 'flipacq' THEN msg ->> 'presetName' END AS flipacq,
    CASE WHEN device = 'stagebs' THEN msg ->> 'presetName' END AS stagebs,
    CASE WHEN device = 'fwpupil' THEN msg ->> 'presetName' END AS fwpupil,
    CASE WHEN device = 'fwfpm' THEN msg ->> 'presetName' END AS fwfpm,
    CASE WHEN device = 'fwlyot' THEN msg ->> 'presetName' END AS fwlyot,
    CASE WHEN device = 'stagescibs' THEN msg ->> 'presetName' END AS stagescibs,
    CASE WHEN device = 'flipwfsf' THEN msg ->> 'presetName' END AS flipwfsf
  FROM telem_partition
  WHERE ts BETWEEN '2024-11-18' AND '2024-11-20'
    
  ),
  filtered_observations AS (
    -- DEDUPLICATE ROWS
    SELECT DISTINCT ON (obsname, observer)
    	ts_utc,
      obsname,
      observer,
      comments_changes,
      holoop_state,
      dimm_fwhm_corr,
      fwsci1,
      camsci1_exptime,
      camsci1_emgain,
      camsci1_read_out_speed,
      camsci1_shutter_state,
      camsci1_roi,
      fwsci2,
      camsci2_exptime,
      camsci2_emgain,
      camsci2_read_out_speed,
      camsci2_shutter_state,
      camsci2_roi,
      camwfs_exptime,
      camwfs_gain,
      flipacq,
      stagebs,
      fwpupil,
      fwfpm,
      fwlyot,
      stagescibs,
      flipwfsf
    FROM combined_data
    WHERE obsname IS NOT NULL
    	OR observer IS NOT NULL  	
    ORDER BY obsname, observer, ts_utc
   ),
   comments_only AS (
     --keep all commetns
     SELECT 
     	ts_utc,
      obsname,
      observer,
      comments_changes,
      holoop_state,
      dimm_fwhm_corr,
      fwsci1,
      camsci1_exptime,
      camsci1_emgain,
      camsci1_read_out_speed,
      camsci1_shutter_state,
      camsci1_roi,
      fwsci2,
      camsci2_exptime,
      camsci2_emgain,
      camsci2_read_out_speed,
      camsci2_shutter_state,
      camsci2_roi,
      camwfs_exptime,
      camwfs_gain,
      flipacq,
      stagebs,
      fwpupil,
      fwfpm,
      fwlyot,
      stagescibs,
      flipwfsf
     FROM combined_data
     WHERE comments_changes IS NOT NULL
   ),
   rest_telem AS (
     SELECT 
     	*
     FROM combined_data
  --   WHERE
  --    holoop_state IS NOT NULL
  --    OR dimm_fwhm_corr IS NOT NULL
  --    OR fwsci1 IS NOT NULL
  --    OR camsci1_exptime IS NOT NULL
  --    OR camsci1_emgain IS NOT NULL
  --    OR camsci1_read_out_speed IS NOT NULL
  --    OR camsci1_shutter_state IS NOT NULL
  --    OR camsci1_roi IS NOT NULL
  --    OR fwsci2 IS NOT NULL
  --    OR camsci2_exptime IS NOT NULL
  --    OR camsci2_emgain IS NOT NULL
  --    OR camsci2_read_out_speed IS NOT NULL
  --    OR camsci2_shutter_state IS NOT NULL
  --    OR camsci2_roi IS NOT NULL
  --    OR camwfs_exptime IS NOT NULL
  --    OR camwfs_gain IS NOT NULL
  --    OR flipacq IS NOT NULL
  --    OR stagebs IS NOT NULL
  --    OR fwpupil IS NOT NULL
  --    OR fwfpm IS NOT NULL
  --    OR fwlyot IS NOT NULL
  --    OR stagescibs IS NOT NULL
  --    OR flipwfsf IS NOT NULL
     )

     
   --combine both back together
  SELECT
  		*
  FROM filtered_observations
  UNION ALL
  SELECT 
			*
	FROM comments_only

  UNION ALL
  
  SELECT
     *
  FROM rest_telem
;
    