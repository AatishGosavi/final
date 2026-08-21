import pool from "../../db/postgres.js";



// for D to A1 conversion
function validateTopBottom(topValue, bottomValue, validator) {

  const hasTop = topValue !== null && topValue !== undefined && topValue !== '';
  const hasBottom = bottomValue !== null && bottomValue !== undefined && bottomValue !== '';

  if (!hasTop && !hasBottom) {
    return false;
  }

  if (hasTop && !validator(parseFloat(topValue))) {
    return false;
  }

  if (hasBottom && !validator(parseFloat(bottomValue))) {
    return false;
  }

  return true;
}

// for D to A1 conversion
function qualifiesForSecondaryProduct(measurement) {

  if (measurement.product_type !== 'G652D250') return false;

  const mac = parseFloat(measurement.mac_value);

  if (isNaN(mac) || mac >= 7.05) return false;

  return (
    validateTopBottom(
      measurement.cut_off_top,
      measurement.cut_off_bottom,
      value => value >= 1270 && value <= 1330
    ) &&

    validateTopBottom(
      measurement.mfd_1310_top,
      measurement.mfd_1310_bottom,
      value => value >= 8.80 && value <= 9.15
    ) &&

    validateTopBottom(
      measurement.secondary_coating_dia_top,
      measurement.secondary_coating_dia_bottom,
      value => value >= 237 && value <= 247
    ) &&

    validateTopBottom(
      measurement.fiber_curl_top,
      measurement.fiber_curl_bottom,
      value => value > 5
    ) &&

    validateTopBottom(
      measurement.clad_ovality_top,
      measurement.clad_ovality_bottom,
      value => value < 0.7
    )
  );

}

// for D to A1 conversion
function hasMbendValues(measurement) {

  const mbendFields = [
   'm_1t_20mm_1550',
    'm_1t_20mm_1625',
    'm_10t_30mm_1625',
    'm_1t_32mm_1550'
    // Add all MBend parameters here
  ];

  return mbendFields.every(field => {
    const value = measurement[field];
    return value !== null && value !== undefined && value !== '';
  });

}

//For D to A1 Conversion
function validateMbend(measurement, gradeSpec) {

  const mbendFields = [
    'm_1t_20mm_1550',
    'm_1t_20mm_1625',
    'm_10t_30mm_1625',
    'm_1t_32mm_1550'
    // Add all MBend parameters here
  ];

  for (const field of mbendFields) {

    const measured = parseFloat(measurement[field]);

    const min = parseFloat(gradeSpec[`min_${field}`]);
    const max = parseFloat(gradeSpec[`max_${field}`]);

    if (!isNaN(min) && measured < min) {
      return false;
    }

    if (!isNaN(max) && measured > max) {
      return false;
    }
  }

  return true;
}


//For D to A1 Conversion


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
export async function validateBobbinQC(bobbinNo) {
  const client = await pool.connect();

  try {
    // A. Fetch the measured data for the bobbin
    const measurementQuery = `SELECT * FROM qc_entry_temp WHERE bobbin_no = $1;`;
    const measurementRes = await client.query(measurementQuery, [bobbinNo]);

    if (measurementRes.rows.length === 0) {
      return { status: 'ERROR', message: `Bobbin ${bobbinNo} not found.` };
    }


    const measurement = measurementRes.rows[0];
    const product_type = measurement.product_type;

    const bobbinRes = await client.query(
    `SELECT fiber_color
     FROM bobbin_entries
     WHERE bobbin_no = $1`,
    [bobbinNo]
);

if (bobbinRes.rows.length === 0) {
    return {
        status: "ERROR",
        message: `Bobbin ${bobbinNo} not found in bobbin_entries`
    };
}

const fiberColor = (bobbinRes.rows[0].fiber_color || "").trim().toUpperCase();

let fiber_type;

if (fiberColor === "NATURAL") {
    fiber_type = "NATURAL";
}
else if (fiberColor.startsWith("RM ")) {
    fiber_type = "RING_MARK";
}
else {
    fiber_type = "COLORED";
}


    const SECONDARY_PRODUCT_TYPE_FOR_LOW_MAC = 'G657A1250C';

    // Fetch D-to-A1 conversion flag from app_config table (dynamic user control)
    const d2a1ConfigRes = await client.query(
      `SELECT config_value FROM app_config WHERE config_key = $1`,
      ['enable_d_to_a1_conversion']
    );
    const enableDtoA1Conversion = d2a1ConfigRes.rows.length > 0
      ? d2a1ConfigRes.rows[0].config_value === 'true'
      : false;

    const useDualProductTypeSpecs = enableDtoA1Conversion
      ? qualifiesForSecondaryProduct(measurement)
      : false;

    
    const effectiveProductType = useDualProductTypeSpecs
      ? SECONDARY_PRODUCT_TYPE_FOR_LOW_MAC
      : product_type;

  
    const parametersToCheckQuery = `Select mandatory_params from grade_mandatory where product_type = $1;`;
    const parametersToCheckRes = await client.query(parametersToCheckQuery, [effectiveProductType])

    const parametersToCheck = parametersToCheckRes.rows[0]?.mandatory_params
      ?.split(',')
      .map(param => param.trim()) || [];


    const fullCheckQuery = `Select full_check from pt_entry where bobbin_no = $1;`;
    const fullCheckRes = await client.query(fullCheckQuery, [bobbinNo]);
    const fullCheck = fullCheckRes.rows.length > 0 ? fullCheckRes.rows[0].full_check === true : false

    // --- NEW LOGIC: Look for missing top/bottom data and copy from whichever side is present ---
    const synchronizedPairsToUpdate = []; // now stores { field, value } to write back to DB

    const isEmpty = (v) => (v === null || v === undefined || v === '');

    // Track which top/bottom pairs had BOTH sides actually measured (before any borrowing below).
    // Used later so a failure on a pair where both sides were genuinely tested is flagged as
    // "Retest" instead of the "Test from Bottom" advice used when only one side was tested.
    const bothSidesPresentMap = {}; // paramName (top or bottom) -> true/false
    for (const pair of topBottomPairs) {
      const bothPresent = !isEmpty(measurement[pair.top]) && !isEmpty(measurement[pair.bottom]);
      bothSidesPresentMap[pair.top] = bothPresent;
      bothSidesPresentMap[pair.bottom] = bothPresent;
    }

    if (!fullCheck) {
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
    }

    // Which fields ended up with a borrowed value (vs. genuinely tested) — used below
    // to label top_value / bottom_value in the failure details as "(borrowed)" or "(tested)".
    const borrowedFields = new Set(synchronizedPairsToUpdate.map(item => item.field));

    //For D to A1 Conversion



    //const macValueParsed = parseFloat(measurement['mac_value']);
    //  const useDualProductTypeSpecs = (product_type === 'G652D250' && !isNaN(macValueParsed) && macValueParsed < 7.05);

    // --------------------------------------------------------------------------

    // B. Fetch all active specifications for this product_type, sorted by priority (1 is best/strictest)
    let specsRes;
    if (useDualProductTypeSpecs) {
      // Result order: matcode='D' tiers first (priority 1,2,3...), then secondary matcode's tiers (priority 1,2,3...)
      const productTypeInOrder = [SECONDARY_PRODUCT_TYPE_FOR_LOW_MAC, product_type];
      const dualSpecsQuery = `
        SELECT * FROM qc_grade 
        WHERE product_type = ANY($1) AND Status = true 
        ORDER BY array_position($1, product_type), priority ASC;
      `;
      specsRes = await client.query(dualSpecsQuery, [productTypeInOrder]);
    } else {
      const specsQuery = `
        SELECT * FROM qc_grade 
        WHERE product_type = $1 AND Status = true AND color_type = $2
        ORDER BY priority ASC;
      `;
      specsRes = await client.query(specsQuery, [product_type,fiber_type ]);
    }


    if (specsRes.rows.length === 0) {
      return { status: 'ERROR', message: `No active specification tiers found for product_type ${product_type}.` };
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

          const pairForParam = isTopBottomParameter(paramName)
            ? topBottomPairs.find(p => p.top === paramName || p.bottom === paramName)
            : null;

          // If one side of this pair was borrowed, the failure should be attributed to
          // the side that was ACTUALLY measured (the source of truth), not to whichever
          // field happened to be checked first in parametersToCheck — and the
          // recommendation should point at the side that's genuinely missing.
          let effectiveFailedParameter = paramName;
          let notice;

          if (pairForParam) {
            const topBorrowed = borrowedFields.has(pairForParam.top);
            const bottomBorrowed = borrowedFields.has(pairForParam.bottom);

            if (topBorrowed) {
              effectiveFailedParameter = pairForParam.bottom; // bottom holds the real measurement
              notice = "Test from Top";
            } else if (bottomBorrowed) {
              effectiveFailedParameter = pairForParam.top; // top holds the real measurement
              notice = "Test from Bottom";
            } else {
              // Both sides were genuinely measured (bothSidesPresentMap true) — no swap needed.
              notice = "Retest";
            }
          } else {
            notice = "Standard parameter mismatch";
          }

          // Recompute measured_value/allowed_range against the field we're actually
          // reporting (identical value to paramName's when borrowed, since that's the
          // whole point of borrowing — this just keeps the range tied to the right field).
          const reportedValue = parseFloat(measurement[effectiveFailedParameter]);
          const reportedMin = parseFloat(tier[`min_${effectiveFailedParameter}`]);
          const reportedMax = parseFloat(tier[`max_${effectiveFailedParameter}`]);

          validationFailureLog = {
            grade_checked: tier.grade,
            priority: tier.priority,
            failed_parameter: effectiveFailedParameter,
            measured_value: reportedValue,
            allowed_range: `[${isNaN(reportedMin) ? '-∞' : reportedMin} to ${isNaN(reportedMax) ? '+∞' : reportedMax}]`,
            recommendation: notice,
            ...(pairForParam && {
              top_value: `${measurement[pairForParam.top]} (${borrowedFields.has(pairForParam.top) ? 'borrowed' : 'tested'})`,
              bottom_value: `${measurement[pairForParam.bottom]} (${borrowedFields.has(pairForParam.bottom) ? 'borrowed' : 'tested'})`
            })
          };

          break;
        }
      }

      // if (tierPassed) {
      //   finalMatchedTier = tier;
      //   validationFailureLog = null;
      //   break;
      // }


      // For D to A1 Conversion
      if (tierPassed) {

        // Only apply MBend logic for secondary product
        if (
          useDualProductTypeSpecs &&
          tier.product_type === SECONDARY_PRODUCT_TYPE_FOR_LOW_MAC
        ) {

          // MBend not entered
          if (!hasMbendValues(measurement)) {

            return {
              status: "MBEND_REQUIRED",
              matched_grade: null,
              matched_priority: null,
              metrics: {
                total_checks_performed: totalChecksPerformed
              },
              message: "Bobbin qualifies for G657A1250. Please complete MBend testing."
            };

          }

          // MBend entered but failed
          if (!validateMbend(measurement, tier)) {

            // Skip this tier and continue checking G652D250
            continue;
          }
        }

        // Normal success
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

      }
      // --------------------------------------------------------------------------

      // --- Upgrade to G657A1250 if either:
      //     (a) bobbin's original product_type was already G657A1250C and it passed, OR
      //     (b) bobbin qualified via the D-to-A1 conversion path (G652D250 -> G657A1250C spec) and passed ---
      const qualifiedViaDtoA1Conversion =
        useDualProductTypeSpecs &&
        finalMatchedTier.product_type === SECONDARY_PRODUCT_TYPE_FOR_LOW_MAC;

      if (product_type === 'G657A1250C' || qualifiedViaDtoA1Conversion) {
        const upgradedProductType = 'G657A1250';

        // Update product_type in qc_entry_temp
        await client.query(
          `UPDATE qc_entry_temp SET product_type = $1 WHERE bobbin_no = $2;`,
          [upgradedProductType, bobbinNo]
        );

        // Update product_type in bobbin_entries
        await client.query(
          `UPDATE bobbin_entries SET product_type = $1 WHERE bobbin_no = $2;`,
          [upgradedProductType, bobbinNo]
        );
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
    await client.release();
  }
}

// 4. Test Runner Routine execution
//async function runTests() {
//  console.log('--- Starting Wide QC Table Dynamic Top/Bottom Sync Tests --- \n');
//
//  const testBobbins = ['B-FAIL-01', 'B-FAIL-02', 'B-FAIL-03', 'B-FAIL-04', 'B-FAIL-05','B-PASS-APLUS','B-PASS-GRADEA','B-PASS-GRADEB','B-PASS-GRADEC'];
//
//  for (const bobbin of testBobbins) {
//    console.log(`Evaluating Spool: ${bobbin}...`);
//    const report = await validateBobbinQC(bobbin);
//    console.log(JSON.stringify(report, null, 2));
//    console.log('\n-------------------------------------------------------\n');
//  }
//}
//
//runTests();