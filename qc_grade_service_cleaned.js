import pool from "../../db/postgres.js";

// ============================================================================
// QC Grade Service
//
// Evaluates a bobbin's recorded measurements against the specification tiers
// for its product type and returns the best grade it qualifies for.
//
// Includes the "D to A1 conversion" path: a G652D250 bobbin with a low MAC
// value can additionally be evaluated against the G657A1250C spec, and on
// success (plus a passing MBend check) gets upgraded to G657A1250.
// ============================================================================

const SECONDARY_PRODUCT_TYPE_FOR_LOW_MAC = 'G657A1250C';
const UPGRADED_PRODUCT_TYPE = 'G657A1250';
const D2A1_SOURCE_PRODUCT_TYPE = 'G652D250';

// Thresholds a G652D250 bobbin must meet to be eligible for evaluation
// against the G657A1250C ("D to A1 conversion") spec.
const D2A1_ELIGIBILITY = {
  MAX_MAC_VALUE: 7.05,
  CUT_OFF: { min: 1270, max: 1330 },
  MFD_1310: { min: 8.80, max: 9.15 },
  SECONDARY_COATING_DIA: { min: 237, max: 247 },
  FIBER_CURL_MIN: 5,
  CLAD_OVALITY_MAX: 0.7
};

// MBend parameters required (and graded) for a bobbin passing on the
// secondary (D-to-A1) product spec. Shared by hasMbendValues/validateMbend
// so the field list only needs to be maintained in one place.
const MBEND_FIELDS = [
  'm_1t_20mm_1550',
  'm_1t_20mm_1625',
  'm_10t_30mm_1625',
  'm_1t_32mm_1550'
  // Add all MBend parameters here
];

// Measurement fields that are taken at two points on the fiber ("top" and
// "bottom"). Used both for the top/bottom-borrowing logic and to decide the
// failure recommendation ("Retest" vs "Test from Bottom").
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

const isEmpty = (v) => (v === null || v === undefined || v === '');

/**
 * True if at least one of topValue/bottomValue is present, and every value
 * that IS present satisfies `validator`. Used to check the D-to-A1
 * eligibility ranges, where either side (or both) may have been measured.
 */
function validateTopBottom(topValue, bottomValue, validator) {
  const hasTop = !isEmpty(topValue);
  const hasBottom = !isEmpty(bottomValue);

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

/**
 * Whether a G652D250 bobbin's measurements fall within range to also be
 * evaluated against the G657A1250C ("D to A1 conversion") spec.
 */
function qualifiesForSecondaryProduct(measurement) {
  if (measurement.product_type !== D2A1_SOURCE_PRODUCT_TYPE) return false;

  const mac = parseFloat(measurement.mac_value);
  if (isNaN(mac) || mac >= D2A1_ELIGIBILITY.MAX_MAC_VALUE) return false;

  return (
    validateTopBottom(
      measurement.cut_off_top,
      measurement.cut_off_bottom,
      value => value >= D2A1_ELIGIBILITY.CUT_OFF.min && value <= D2A1_ELIGIBILITY.CUT_OFF.max
    ) &&
    validateTopBottom(
      measurement.mfd_1310_top,
      measurement.mfd_1310_bottom,
      value => value >= D2A1_ELIGIBILITY.MFD_1310.min && value <= D2A1_ELIGIBILITY.MFD_1310.max
    ) &&
    validateTopBottom(
      measurement.secondary_coating_dia_top,
      measurement.secondary_coating_dia_bottom,
      value => value >= D2A1_ELIGIBILITY.SECONDARY_COATING_DIA.min && value <= D2A1_ELIGIBILITY.SECONDARY_COATING_DIA.max
    ) &&
    validateTopBottom(
      measurement.fiber_curl_top,
      measurement.fiber_curl_bottom,
      value => value > D2A1_ELIGIBILITY.FIBER_CURL_MIN
    ) &&
    validateTopBottom(
      measurement.clad_ovality_top,
      measurement.clad_ovality_bottom,
      value => value < D2A1_ELIGIBILITY.CLAD_OVALITY_MAX
    )
  );
}

/** True if every required MBend field has a value entered. */
function hasMbendValues(measurement) {
  return MBEND_FIELDS.every(field => !isEmpty(measurement[field]));
}

/** True if every MBend field's measured value is within the tier's min/max bounds. */
function validateMbend(measurement, gradeSpec) {
  for (const field of MBEND_FIELDS) {
    const measured = parseFloat(measurement[field]);
    const min = parseFloat(gradeSpec[`min_${field}`]);
    const max = parseFloat(gradeSpec[`max_${field}`]);

    if (!isNaN(min) && measured < min) return false;
    if (!isNaN(max) && measured > max) return false;
  }
  return true;
}

/** True if `paramName` is the top or bottom field of one of the top/bottom pairs. */
function isTopBottomParameter(paramName) {
  return topBottomPairs.some(pair => pair.top === paramName || pair.bottom === paramName);
}

/**
 * For measurement pairs missing one side (top or bottom), copies the present
 * side's value into the missing one so grading can proceed on a single
 * tested reading. Only runs when `fullCheck` is false. Mutates `measurement`
 * in place and returns the list of { field, value } writes to persist later.
 */
function synchronizeTopBottomPairs(measurement, fullCheck) {
  const synchronizedPairsToUpdate = [];
  if (!fullCheck) {
    for (const pair of topBottomPairs) {
      const topVal = measurement[pair.top];
      const bottomVal = measurement[pair.bottom];

      if (isEmpty(bottomVal) && !isEmpty(topVal)) {
        measurement[pair.bottom] = topVal;
        synchronizedPairsToUpdate.push({ field: pair.bottom, value: topVal });
      } else if (isEmpty(topVal) && !isEmpty(bottomVal)) {
        measurement[pair.top] = bottomVal;
        synchronizedPairsToUpdate.push({ field: pair.top, value: bottomVal });
      }
    }
  }
  return synchronizedPairsToUpdate;
}

/**
 * Snapshot of which top/bottom pairs had BOTH sides genuinely measured,
 * taken BEFORE synchronizeTopBottomPairs() runs (after borrowing, both
 * sides always look present). Keyed by field name (top or bottom) so a
 * failure on either side of a pair can look up the same true/false.
 */
function buildBothSidesPresentMap(measurement) {
  const map = {};
  for (const pair of topBottomPairs) {
    const bothPresent = !isEmpty(measurement[pair.top]) && !isEmpty(measurement[pair.bottom]);
    map[pair.top] = bothPresent;
    map[pair.bottom] = bothPresent;
  }
  return map;
}

/** Determines fiber_type from the bobbin's stored fiber_color. */
function deriveFiberType(fiberColor) {
  const normalized = (fiberColor || '').trim().toUpperCase();
  if (normalized === 'NATURAL') return 'NATURAL';
  if (normalized.startsWith('RM ')) return 'RING_MARK';
  return 'COLORED';
}

/**
 * Evaluates a bobbin's recorded QC measurements against its product type's
 * specification tiers and returns the matched grade, or the reason it
 * didn't match (missing data, failed checks, MBend required, etc).
 */
export async function validateBobbinQC(bobbinNo) {
  const client = await pool.connect();

  try {
    // A. Fetch the measured data for the bobbin
    const measurementRes = await client.query(
      `SELECT * FROM qc_entry_temp WHERE bobbin_no = $1;`,
      [bobbinNo]
    );

    if (measurementRes.rows.length === 0) {
      return { status: 'ERROR', message: `Bobbin ${bobbinNo} not found.` };
    }

    const measurement = measurementRes.rows[0];
    const product_type = measurement.product_type;

    // These three reads are independent of one another (fiber color, the
    // D-to-A1 feature flag, and the full-check flag), so they're fetched
    // together instead of one after another.
    const [bobbinRes, d2a1ConfigRes, fullCheckRes] = await Promise.all([
      client.query(
        `SELECT fiber_color FROM bobbin_entries WHERE bobbin_no = $1`,
        [bobbinNo]
      ),
      client.query(
        `SELECT config_value FROM app_config WHERE config_key = $1`,
        ['enable_d_to_a1_conversion']
      ),
      client.query(
        `Select full_check from pt_entry where bobbin_no = $1;`,
        [bobbinNo]
      )
    ]);

    if (bobbinRes.rows.length === 0) {
      return { status: 'ERROR', message: `Bobbin ${bobbinNo} not found in bobbin_entries` };
    }

    const fiber_type = deriveFiberType(bobbinRes.rows[0].fiber_color);
    const fullCheck = fullCheckRes.rows.length > 0 ? fullCheckRes.rows[0].full_check === true : false;

    // Fetch D-to-A1 conversion flag from app_config table (dynamic user control)
    const enableDtoA1Conversion = d2a1ConfigRes.rows.length > 0
      ? d2a1ConfigRes.rows[0].config_value === 'true'
      : false;

    const useDualProductTypeSpecs = enableDtoA1Conversion
      ? qualifiesForSecondaryProduct(measurement)
      : false;

    const effectiveProductType = useDualProductTypeSpecs
      ? SECONDARY_PRODUCT_TYPE_FOR_LOW_MAC
      : product_type;

    const parametersToCheckRes = await client.query(
      `Select mandatory_params from grade_mandatory where product_type = $1;`,
      [effectiveProductType]
    );

    const parametersToCheck = parametersToCheckRes.rows[0]?.mandatory_params
      ?.split(',')
      .map(param => param.trim()) || [];

    // Snapshot which pairs had both sides genuinely measured BEFORE borrowing,
    // then borrow any missing side from its present pair partner.
    const bothSidesPresentMap = buildBothSidesPresentMap(measurement);
    const synchronizedPairsToUpdate = synchronizeTopBottomPairs(measurement, fullCheck);

    // Which fields ended up with a borrowed value (vs. genuinely tested) — used below
    // to label top_value / bottom_value in the failure details as "(borrowed)" or "(tested)".
    const borrowedFields = new Set(synchronizedPairsToUpdate.map(item => item.field));

    // B. Fetch all active specifications for this product_type, sorted by priority (1 is best/strictest)
    let specsRes;
    if (useDualProductTypeSpecs) {
      // Result order: matcode='D' tiers first (priority 1,2,3...), then secondary matcode's tiers (priority 1,2,3...)
      const productTypeInOrder = [SECONDARY_PRODUCT_TYPE_FOR_LOW_MAC, product_type];
      specsRes = await client.query(
        `SELECT * FROM qc_grade 
         WHERE product_type = ANY($1) AND Status = true 
         ORDER BY array_position($1, product_type), priority ASC;`,
        [productTypeInOrder]
      );
    } else {
      specsRes = await client.query(
        `SELECT * FROM qc_grade 
         WHERE product_type = $1 AND Status = true AND color_type = $2
         ORDER BY priority ASC;`,
        [product_type, fiber_type]
      );
    }

    if (specsRes.rows.length === 0) {
      return { status: 'ERROR', message: `No active specification tiers found for product_type ${product_type}.` };
    }

    const specificationTiers = specsRes.rows;

    // Determine which parameters are still null/missing AFTER top-bottom sync.
    // If ANY parameter is missing, we do NOT enter the grade-check loop at all for ANY tier -
    // we just report back which parameters are missing so the operator knows what to test.
    const missingParameters = parametersToCheck.filter(paramName => isEmpty(measurement[paramName]));

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

          // Determine advice message based on whether a top/bottom field failed bounds validation
          const notice = isTopBottomParameter(paramName)
            ? (bothSidesPresentMap[paramName] ? "Retest" : "Test from Bottom")
            : "Standard parameter mismatch";

          // For top/bottom parameters, also surface both sides' values (each labeled
          // "tested" or "borrowed") instead of only the single side that failed.
          const pairForParam = isTopBottomParameter(paramName)
            ? topBottomPairs.find(p => p.top === paramName || p.bottom === paramName)
            : null;

          validationFailureLog = {
            grade_checked: tier.grade,
            priority: tier.priority,
            failed_parameter: paramName,
            measured_value: measuredValue,
            allowed_range: `[${isNaN(minAllowed) ? '-∞' : minAllowed} to ${isNaN(maxAllowed) ? '+∞' : maxAllowed}]`,
            recommendation: notice,
            ...(pairForParam && {
              top_value: `${measurement[pairForParam.top]} (${borrowedFields.has(pairForParam.top) ? 'borrowed' : 'tested'})`,
              bottom_value: `${measurement[pairForParam.bottom]} (${borrowedFields.has(pairForParam.bottom) ? 'borrowed' : 'tested'})`
            })
          };

          break;
        }
      }

      if (tierPassed) {
        // Only apply MBend logic for secondary product
        if (useDualProductTypeSpecs && tier.product_type === SECONDARY_PRODUCT_TYPE_FOR_LOW_MAC) {
          // MBend not entered
          if (!hasMbendValues(measurement)) {
            return {
              status: "MBEND_REQUIRED",
              matched_grade: null,
              matched_priority: null,
              metrics: { total_checks_performed: totalChecksPerformed },
              message: "Bobbin qualifies for G657A1250. Please complete MBend testing."
            };
          }

          // MBend entered but failed — skip this tier and continue checking G652D250
          if (!validateMbend(measurement, tier)) {
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
      // If passing and values were borrowed, update the permanent table
      if (synchronizedPairsToUpdate.length > 0) {
        const queryParams = [bobbinNo];
        const updateFields = synchronizedPairsToUpdate.map((item, index) => {
          queryParams.push(item.value);
          return `${item.field} = $${index + 2}`;
        });

        await client.query(
          `UPDATE qc_entry SET ${updateFields.join(', ')} WHERE bobbin_no = $1;`,
          queryParams
        );
      }

      // Upgrade to G657A1250 if either:
      //   (a) bobbin's original product_type was already G657A1250C and it passed, OR
      //   (b) bobbin qualified via the D-to-A1 conversion path (G652D250 -> G657A1250C spec) and passed
      const qualifiedViaDtoA1Conversion =
        useDualProductTypeSpecs && finalMatchedTier.product_type === SECONDARY_PRODUCT_TYPE_FOR_LOW_MAC;

      if (product_type === SECONDARY_PRODUCT_TYPE_FOR_LOW_MAC || qualifiedViaDtoA1Conversion) {
        await client.query(
          `UPDATE qc_entry_temp SET product_type = $1 WHERE bobbin_no = $2;`,
          [UPGRADED_PRODUCT_TYPE, bobbinNo]
        );
        await client.query(
          `UPDATE bobbin_entries SET product_type = $1 WHERE bobbin_no = $2;`,
          [UPGRADED_PRODUCT_TYPE, bobbinNo]
        );
      }

      return {
        status: 'PASSED',
        matched_grade: finalMatchedTier.grade,
        matched_priority: finalMatchedTier.priority,
        metrics: { total_checks_performed: totalChecksPerformed },
        missing_parameters: missingParameters, // params null/missing after top-bottom sync, excluded from grade check
        failure_details: null
      };
    }

    return {
      status: 'FAILED',
      matched_grade: null,
      matched_priority: null,
      metrics: { total_checks_performed: totalChecksPerformed },
      missing_parameters: missingParameters, // params null/missing after top-bottom sync, excluded from grade check
      failure_details: validationFailureLog
    };

  } catch (error) {
    console.error('Validation Script Runtime Exception:', error);
    return { status: 'CRITICAL_ERROR', message: error.message };
  } finally {
    await client.release();
  }
}
