SELECT * FROM user_log
WHERE ec = 'user_log'
AND
ts > now() - interval '2 days'
ORDER BY ts DESC

limit 20;



CREATE INDEX idx_user_log_ts ON user_log (ts);

-- Step 1: Identify Observing Periods
WITH observing_periods AS (
    SELECT 
        ts AS observing_start,
        LEAD(ts) OVER (ORDER BY ts) AS observing_end
    FROM telem_2024_11
    WHERE device = 'observers' AND msg ->> 'observing' = 'true'
),

-- Step 2: Retrieve Data for All Devices Within Observing Periods
filtered_data AS (
    SELECT t.*
    FROM telem_partition t
    JOIN observing_periods op 
      ON t.ts BETWEEN op.observing_start AND op.observing_end
)

-- Step 3: Extract Fields Based on Device
SELECT 
    TO_CHAR(ts, 'YYYY-MM-DD HH24:MI:SS') AS ts_utc,
    CASE WHEN device = 'observers' THEN msg ->> 'obsName' END AS obsname,
    CASE WHEN device = 'holoop' THEN msg ->> 'state' END AS holoop_state,
    CASE WHEN device = 'tcsi' AND ec = 'telem_telsee' THEN msg ->> 'dimm_fwhm_corr' END AS dimm_fwhm_corr,
    CASE WHEN device = 'fwsci1' THEN msg ->> 'presetName' END AS fwsci1,
    CASE WHEN device = 'camsci1' AND ec = 'telem_stdcam' THEN msg ->> 'exptime' END AS camsci1_exptime,
    CASE WHEN device = 'camsci1' AND ec = 'telem_stdcam' THEN msg ->> 'emGain' END AS camsci1_emgain,
    CASE WHEN device = 'camsci1' AND ec = 'telem_stdcam' THEN msg ->> 'adcSpeed' END AS camsci1_read_out_speed,
    CASE WHEN device = 'camsci1' AND ec = 'telem_stdcam' THEN (msg -> 'shutter' ->> 'state') END AS camsci1_shutter_state,
    CASE WHEN device = 'camsci1' AND ec = 'telem_stdcam' THEN (msg -> 'roi' ->> 'h')::text END || 'x' ||
        CASE WHEN device = 'camsci1' AND ec = 'telem_stdcam' THEN (msg -> 'roi' ->> 'w')::text END AS camsci1_roi,
    CASE WHEN device = 'fwsci2' THEN msg ->> 'presetName' END AS fwsci2,
    CASE WHEN device = 'camsci2' AND ec = 'telem_stdcam' THEN msg ->> 'exptime' END AS camsci2_exptime,
    CASE WHEN device = 'camsci2' AND ec = 'telem_stdcam' THEN msg ->> 'emGain' END AS camsci2_emgain,
    CASE WHEN device = 'camsci2' AND ec = 'telem_stdcam' THEN msg ->> 'adcSpeed' END AS camsci2_read_out_speed,
    CASE WHEN device = 'camsci2' AND ec = 'telem_stdcam' THEN (msg -> 'shutter' ->> 'state') END AS camsci2_shutter_state,
    CASE WHEN device = 'camsci2' AND ec = 'telem_stdcam' THEN (msg -> 'roi' ->> 'h')::text END || 'x' ||
        CASE WHEN device = 'camsci2' AND ec = 'telem_stdcam' THEN (msg -> 'roi' ->> 'w')::text END AS camsci2_roi,
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
ORDER BY ts;




