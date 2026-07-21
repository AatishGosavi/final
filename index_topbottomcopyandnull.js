const { Client } = require('pg');

// 1. Database Connection Configuration
const dbConfig = {
  user: 'postgres',
  host: 'localhost',
  database: 'postgres',
  password: 'naxnova', 
  port: 5432,
};

// 2. Full list of parameters to dynamically loop through
const parametersToCheck = [
  'avg_lsa_atn_1310', 'avg_lsa_atn_1550', 'avg_lsa_atn_1625', 'avg_lsa_atn_1383',
  'max_lsa_atn_1310', 'max_lsa_atn_1550', 'max_lsa_atn_1625', 'max_lsa_atn_1383',
  'min_lsa_atn_1310', 'min_lsa_atn_1550', 'min_lsa_atn_1625', 'min_lsa_atn_1383',
  'atn_1310_top', 'atn_1550_top', 'atn_1625_top', 'atn_1383_top',
  'atn_1310_bottom', 'atn_1550_bottom', 'atn_1625_bottom', 'atn_1383_bottom',
  'max_atn_1310_top', 'max_atn_1550_top', 'max_atn_1625_top', 'max_atn_1383_top',
  'max_atn_1310_bottom', 'max_atn_1550_bottom', 'max_atn_1625_bottom', 'max_atn_1383_bottom',
  'max_tb_1310', 'max_tb_1550', 'max_tb_1625', 'max_tb_1383',
  'atn_1310_tb', 'atn_1550_tb', 'atn_1625_tb', 'atn_1383_tb',
  'atn_uniformity_1310', 'atn_uniformity_1550', 'atn_uniformity_1625', 'atn_uniformity_1383',
  'mfd_uniformity_1310', 'mfd_uniformity_1550', 'mfd_uniformity_1625', 'mfd_uniformity_1383',
  'step_1310_size', 'step_1550_size', 'step_1625_size', 'step_1383_size',
  'spike_1310_size', 'spike_1550_size', 'spike_1625_size', 'spike_1383_size',
  'spec_1310', 'spec_1550', 'spec_1285_1330',
  'mfd_1310_top', 'mfd_1310_bottom', 'mfd_1550_top', 'mfd_1550_bottom',
  'effective_area_1310', 'effective_area_1550',
  'cut_off_top', 'cut_off_bottom', 'cable_cut_off', 'mac_value',
  'clad_dia_top', 'clad_dia_bottom', 'core_clad_concentricity_top', 'core_clad_concentricity_bottom',
  'clad_ovality_top', 'clad_ovality_bottom', 'core_dia_top', 'core_dia_bottom',
  'core_ovality_top', 'core_ovality_bottom', 'primary_coating_dia_top', 'primary_coating_dia_bottom',
  'secondary_coating_dia_top', 'secondary_coating_dia_bottom', 'primary_coating_concentricity_top', 'primary_coating_concentricity_bottom',
  'secondary_coating_concentricity_top', 'secondary_coating_concentricity_bottom', 'coating_ovality_top', 'coating_ovality_bottom',
  'fiber_curl_top', 'fiber_curl_bottom', 'curl_defection_top', 'curl_defection_bottom',
  'zero_disp_wave', 'slope_zero_disp', 'disp_1550', 'disp_1285_1330', 'disp_1270_1340', 'disp_1575',
  'cd_1460', 'disp_1625', 'disp_1570', 'disp_1260', 'pmd_1310', 'pmd_1550', 'disp_slope',
  'm_100T_50mm_1550', 'm_100T_50mm_1310', 'm_100T_50mm_1625',
  'm_100T_60mm_1550', 'm_100T_60mm_1310', 'm_100T_60mm_1625',
  'm_1T_32mm_1550', 'm_1T_32mm_1310', 'm_1T_32mm_1625',
  'm_10T_30mm_1550', 'm_10T_30mm_1310', 'm_10T_30mm_1625',
  'm_1T_20mm_1550', 'm_1T_20mm_1310', 'm_1T_20mm_1625',
  'm_1T_15mm_1550', 'm_1T_15mm_1310', 'm_1T_15mm_1625',
  'm_1T_10mm_1550', 'm_1T_10mm_1310', 'm_1T_10mm_1625'
];

// Defined Top/Bottom Mapping Pairs for processing rules
const topBottomPairs = [
  { top: 'mfd_1310_top', bottom: 'mfd_1310_bottom' },
  { top: 'mfd_1550_top', bottom: 'mfd_1550_bottom' },
  { top: 'cut_off_top', bottom: 'cut_off_bottom' },
  { top: 'clad_dia_top', bottom: 'clad_dia_bottom' },
  { top: 'core_clad_concentricity_top', bottom: 'core_clad_concentricity_bottom' },
  { top: 'clad_ovality_top', bottom: 'clad_ovality_bottom' },
  { top: 'core_dia_top', bottom: 'core_dia_bottom' },
  { top: 'core_ovality_top', bottom: 'core_ovality_bottom' },
  { top: 'primary_coating_dia_top', bottom: 'primary_coating_dia_bottom' },
  { top: 'secondary_coating_dia_top', bottom: 'secondary_coating_dia_bottom' },
  { top: 'primary_coating_concentricity_top', bottom: 'primary_coating_concentricity_bottom' },
  { top: 'secondary_coating_concentricity_top', bottom: 'secondary_coating_concentricity_bottom' },
  { top: 'coating_ovality_top', bottom: 'coating_ovality_bottom' },
  { top: 'fiber_curl_top', bottom: 'fiber_curl_bottom' },
  { top: 'curl_defection_top', bottom: 'curl_defection_bottom' }
];

// Helper to determine if a specific string parameter represents a Top/Bottom rule group
function isTopBottomParameter(paramName) {
  return topBottomPairs.some(pair => pair.top === paramName || pair.bottom === paramName);
}

// 3. Core Validation Engine Function
async function validateBobbinQC(bobbinNo) {
  const client = new Client(dbConfig);
  await client.connect();

  try {
    // A. Fetch the measured data for the bobbin
    const measurementQuery = `SELECT * FROM qc_entry WHERE bobbin_no = $1;`;
    const measurementRes = await client.query(measurementQuery, [bobbinNo]);

    if (measurementRes.rows.length === 0) {
      return { status: 'ERROR', message: `Bobbin ${bobbinNo} not found.` };
    }

    const measurement = measurementRes.rows[0];
    const matcode = measurement.matcode;

    // --- NEW LOGIC: Look for missing top/bottom data and copy from whichever side is present ---
    const synchronizedPairsToUpdate = []; // now stores { field, value } to write back to DB

    const isEmpty = (v) => (v === null || v === undefined || v === '');

    for (const pair of topBottomPairs) {
      const topVal = measurement[pair.top];
      const bottomVal = measurement[pair.bottom];

      // If bottom value is missing/null, but top value exists, borrow top value for testing
      if (isEmpty(bottomVal) && !isEmpty(topVal)) {
        measurement[pair.bottom] = topVal;
        synchronizedPairsToUpdate.push({ field: pair.bottom, value: topVal });
      }
      // If top value is missing/null, but bottom value exists, borrow bottom value for testing
      else if (isEmpty(topVal) && !isEmpty(bottomVal)) {
        measurement[pair.top] = bottomVal;
        synchronizedPairsToUpdate.push({ field: pair.top, value: bottomVal });
      }
    }
    // --------------------------------------------------------------------------

    // B. Fetch all active specifications for this Matcode, sorted by priority (1 is best/strictest)
    const specsQuery = `
      SELECT * FROM qc_grade 
      WHERE Matcode = $1 AND Status = true 
      ORDER BY priority ASC;
    `;
    const specsRes = await client.query(specsQuery, [matcode]);

    if (specsRes.rows.length === 0) {
      return { status: 'ERROR', message: `No active specification tiers found for Matcode ${matcode}.` };
    }

    const specificationTiers = specsRes.rows;

    // --- NEW LOGIC: Determine which parameters are still null/missing AFTER top-bottom sync.
    // If ANY parameter is missing, we do NOT enter the grade-check loop at all for ANY tier -
    // we just report back which parameters are missing so the operator knows what to test. ---
    const missingParameters = [];
    for (const paramName of parametersToCheck) {
      const rawValue = measurement[paramName];
      if (rawValue === null || rawValue === undefined || rawValue === '') {
        missingParameters.push(paramName);
      }
    }

    if (missingParameters.length > 0) {
      return {
        status: 'MISSING_DATA',
        matched_grade: null,
        matched_priority: null,
        metrics: { total_checks_performed: 0 },
        missing_parameters: missingParameters, // params null/missing after top-bottom sync - no grade check was run
        failure_details: null
      };
    }
    // --------------------------------------------------------------------------

    let totalChecksPerformed = 0;
    let finalMatchedTier = null;
    let validationFailureLog = null;

    // C. OUTER LOOP: Iterate over each Priority Tier (Grade A+, Grade A, etc.)
    for (const tier of specificationTiers) {
      let tierPassed = true;

      // D. INNER LOOP: Check every single parameter against this tier's rules
      for (const paramName of parametersToCheck) {

        totalChecksPerformed++;

        const measuredValue = parseFloat(measurement[paramName]);
        const minAllowed = parseFloat(tier[`min_${paramName}`]);
        const maxAllowed = parseFloat(tier[`max_${paramName}`]);

        const passesMin = isNaN(minAllowed) || measuredValue >= minAllowed;
        const passesMax = isNaN(maxAllowed) || measuredValue <= maxAllowed;

        if (!passesMin || !passesMax) {
          tierPassed = false;
          
          // Determine advice message based on whether a top/bottom field failed bounds validation
          const notice = isTopBottomParameter(paramName) ? "Test from Bottom" : "Standard parameter mismatch";

          validationFailureLog = {
            grade_checked: tier.grade,
            priority: tier.priority,
            failed_parameter: paramName,
            measured_value: measuredValue,
            allowed_range: `[${isNaN(minAllowed) ? '-∞' : minAllowed} to ${isNaN(maxAllowed) ? '+∞' : maxAllowed}]`,
            recommendation: notice
          };

          break;
        }
      }

      if (tierPassed) {
        finalMatchedTier = tier;
        validationFailureLog = null;
        break;
      }
    }

    // E. Structure Final Result Payload & Save Updates
    if (finalMatchedTier) {
      
      // --- NEW LOGIC: If passing and values were borrowed, update the table ---
      if (synchronizedPairsToUpdate.length > 0) {
        let updateFields = [];
        let queryParams = [bobbinNo];
        let placeholderIndex = 2;

        for (const item of synchronizedPairsToUpdate) {
          updateFields.push(`${item.field} = $${placeholderIndex}`);
          queryParams.push(item.value);
          placeholderIndex++;
        }

        const updateQuery = `
          UPDATE qc_entry 
          SET ${updateFields.join(', ')} 
          WHERE bobbin_no = $1;
        `;
        
        await client.query(updateQuery, queryParams);
        console.log(`[DB Sync] Successfully copied missing top values into bottom rows for ${bobbinNo}.`);
      }
      // --------------------------------------------------------------------------

      return {
        status: 'PASSED',
        matched_grade: finalMatchedTier.grade,
        matched_priority: finalMatchedTier.priority,
        metrics: { total_checks_performed: totalChecksPerformed },
        missing_parameters: missingParameters, // NEW: params null/missing after top-bottom sync, excluded from grade check
        failure_details: null
      };
    } else {
      return {
        status: 'FAILED',
        matched_grade: null,
        matched_priority: null,
        metrics: { total_checks_performed: totalChecksPerformed },
        missing_parameters: missingParameters, // NEW: params null/missing after top-bottom sync, excluded from grade check
        failure_details: validationFailureLog
      };
    }

  } catch (error) {
    console.error('Validation Script Runtime Exception:', error);
    return { status: 'CRITICAL_ERROR', message: error.message };
  } finally {
    await client.end();
  }
}

// 4. Test Runner Routine execution
async function runTests() {
  console.log('--- Starting Wide QC Table Dynamic Top/Bottom Sync Tests --- \n');
  
  const testBobbins = ['B-FAIL-01', 'B-FAIL-02', 'B-FAIL-03', 'B-FAIL-04', 'B-FAIL-05','B-PASS-APLUS','B-PASS-GRADEA','B-PASS-GRADEB','B-PASS-GRADEC'];

  for (const bobbin of testBobbins) {
    console.log(`Evaluating Spool: ${bobbin}...`);
    const report = await validateBobbinQC(bobbin);
    console.log(JSON.stringify(report, null, 2));
    console.log('\n-------------------------------------------------------\n');
  }
}

runTests();