import numpy as np
import pandas as pd
from dataclasses import dataclass

def quarter_start(year: int, quarter: int) -> pd.Timestamp:
    m = {1: 1, 2: 4, 3: 7, 4: 10}[quarter]
    return pd.Timestamp(year=year, month=m, day=1)

def safe_ts(x) -> pd.Timestamp:
    return pd.to_datetime(x, errors="coerce")

@dataclass
class InpatientMessyConfig:
    # Scale
    n_people: int = 10_000
    start_month: str = "2022-01-01"
    n_months: int = 24
    seed: int = 7

    # Volume controls
    p_any_stay_per_person: float = 0.25
    min_stays: int = 1
    max_stays: int = 2
    min_los: int = 1
    max_los: int = 7

    # Claim splitting and messiness
    p_split_into_interim: float = 0.55
    p_duplicate_header_row: float = 0.02
    p_duplicate_line_row: float = 0.015
    p_header_date_inconsistency: float = 0.08
    p_line_date_out_of_bounds: float = 0.06
    p_transfer_overlap: float = 0.10
    p_denied_claim: float = 0.05

    # Safety caps for exploding date ranges
    max_expand_days_per_line: int = 31

def make_people(n_people: int) -> pd.DataFrame:
    return pd.DataFrame({"person_id": [f"P{str(i).zfill(6)}" for i in range(1, n_people + 1)]})

def make_msis_ids(persons: pd.DataFrame) -> pd.DataFrame:
    out = persons.copy()
    out["MSIS_ID"] = out["person_id"].map(lambda x: "MSIS" + x[1:])
    out["SUBMTG_STATE_CD"] = "05"
    out["STATE_CD"] = "AR"
    return out[["person_id", "MSIS_ID", "SUBMTG_STATE_CD", "STATE_CD"]]

def generate_messy_inpatient_claims(cfg: InpatientMessyConfig):
    rng = np.random.default_rng(cfg.seed)

    persons = make_people(cfg.n_people)
    xwalk = make_msis_ids(persons)

    months = pd.date_range(pd.Timestamp(cfg.start_month), periods=cfg.n_months, freq="MS")

    drg_pool = ["291", "775", "470", "392", "194", "690"]
    bill_type_pool = ["0111", "0112", "0113", "0114", "0131"]
    dx_pool = ["E119", "I10", "F329", "J189", "M545"]
    npi_low, npi_high = 1000000000, 9999999999

    rev_routine = ["0100", "0110", "0120"]
    rev_icu = ["0200", "0201", "0206"]
    rev_pharm = ["0250"]
    rev_lab = ["0300"]
    rev_er = ["0450"]
    rev_other = ["0987"]

    # Build "true" stays first, then emit messy claims
    true_stays_rows = []
    for _, row in xwalk.iterrows():
        if rng.random() > cfg.p_any_stay_per_person:
            continue

        n_stays = int(rng.integers(cfg.min_stays, cfg.max_stays + 1))
        start_idx = rng.choice(len(months) - 2, size=n_stays, replace=False)
        start_idx.sort()

        last_end = None
        for si in start_idx:
            base_start = months[si] + pd.Timedelta(days=int(rng.integers(0, 25)))
            if last_end is not None and base_start <= last_end + pd.Timedelta(days=2):
                base_start = last_end + pd.Timedelta(days=int(rng.integers(3, 20)))

            los = int(rng.integers(cfg.min_los, cfg.max_los + 1))
            admit = base_start.normalize()
            discharge = (admit + pd.Timedelta(days=los)).normalize()

            true_stays_rows.append({
                "MSIS_ID": row["MSIS_ID"],
                "SUBMTG_STATE_CD": row["SUBMTG_STATE_CD"],
                "STATE_CD": row["STATE_CD"],
                "true_admit_dt": admit.date().isoformat(),
                "true_discharge_dt": discharge.date().isoformat(),
                "true_los": los,
                "true_drg_cd": rng.choice(drg_pool),
                "true_dx_cd_1": rng.choice(dx_pool),
                "true_prvdr_npi": str(int(rng.integers(npi_low, npi_high))),
            })
            last_end = discharge

    true_stays = pd.DataFrame(true_stays_rows)

    header_rows = []
    line_rows = []
    clm_counter = 1

    for _, stay in true_stays.iterrows():
        admit = safe_ts(stay["true_admit_dt"])
        discharge = safe_ts(stay["true_discharge_dt"])
        los = int(stay["true_los"])
        msis_id = stay["MSIS_ID"]

        do_transfer = (rng.random() < cfg.p_transfer_overlap) and (los >= 4)

        if rng.random() < cfg.p_split_into_interim:
            n_claims = int(rng.choice([2, 3], p=[0.75, 0.25]))
        else:
            n_claims = 1

        segs = []
        if n_claims == 1:
            segs = [(admit, discharge)]
        elif n_claims == 2:
            mid = admit + pd.Timedelta(days=int(max(1, los // 2)))
            segs = [(admit, mid), (mid, discharge)]
        else:
            d1 = admit + pd.Timedelta(days=int(max(1, los // 3)))
            d2 = admit + pd.Timedelta(days=int(max(2, 2 * los // 3)))
            segs = [(admit, d1), (d1, d2), (d2, discharge)]

        if do_transfer and len(segs) >= 2:
            a2, b2 = segs[1]
            overlap = pd.Timedelta(days=int(rng.integers(1, 3)))
            segs[1] = (max(admit, a2 - overlap), b2)

        total_allowed = float(max(0, rng.normal(12000, 4500)))
        total_paid = float(max(0, total_allowed * rng.uniform(0.55, 0.98)))

        claim_weights = rng.random(len(segs))
        claim_weights = claim_weights / claim_weights.sum()

        for j, (seg_start, seg_end) in enumerate(segs, start=1):
            clm_id = f"IPCLM{clm_counter:09d}"
            clm_counter += 1

            denied = int(rng.random() < cfg.p_denied_claim)
            allowed = float(total_allowed * claim_weights[j - 1])
            paid = 0.0 if denied == 1 else float(total_paid * claim_weights[j - 1])

            bill_type = "0111" if (j == 1 and len(segs) == 1) else rng.choice(bill_type_pool)
            if len(segs) > 1:
                if j < len(segs):
                    bill_type = "0112"
                else:
                    bill_type = rng.choice(["0114", "0113", "0112"], p=[0.55, 0.25, 0.20])

            hdr_admit = seg_start
            hdr_discharge = seg_end

            if rng.random() < cfg.p_header_date_inconsistency:
                if rng.random() < 0.5:
                    hdr_discharge = hdr_discharge + pd.Timedelta(days=1)
                else:
                    hdr_discharge = max(hdr_admit, hdr_discharge - pd.Timedelta(days=1))

            header_rows.append({
                "CLM_ID": clm_id,
                "MSIS_ID": msis_id,
                "SUBMTG_STATE_CD": stay["SUBMTG_STATE_CD"],
                "STATE_CD": stay["STATE_CD"],
                "ADMIT_DT": hdr_admit.date().isoformat(),
                "DISCH_DT": hdr_discharge.date().isoformat(),
                "BILL_TYPE": bill_type,
                "DRG_CD": stay["true_drg_cd"],
                "DX_CD_1": stay["true_dx_cd_1"],
                "PRVDR_NPI": stay["true_prvdr_npi"],
                "TOTAL_CHARGES": round(allowed * rng.uniform(1.2, 2.8), 2),
                "ALLOWED_AMT": round(allowed, 2),
                "PAID_AMT": round(paid, 2),
                "DENIED_IND": denied,
            })

            seg_days = max(0, (seg_end - seg_start).days)
            n_other_lines = int(rng.choice([2, 3, 4], p=[0.45, 0.35, 0.20]))

            icu_days = 0
            if rng.random() < 0.30 and seg_days >= 2:
                icu_days = int(rng.integers(1, min(3, seg_days) + 1))

            routine_units = seg_days
            if rng.random() < 0.12:
                routine_units = max(0, routine_units + int(rng.choice([-1, 1, 2])))

            n_lines = 1 + (1 if icu_days > 0 else 0) + n_other_lines
            weights = rng.random(n_lines)
            weights = weights / weights.sum()

            line_num = 1

            routine_bgn = seg_start
            routine_end = seg_end
            if rng.random() < cfg.p_line_date_out_of_bounds:
                routine_end = routine_end + pd.Timedelta(days=1)

            line_rows.append({
                "CLM_ID": clm_id,
                "LINE_NUM": line_num,
                "REV_CNTR_CD": rng.choice(rev_routine),
                "LINE_SRVC_BGN_DT": routine_bgn.date().isoformat(),
                "LINE_SRVC_END_DT": routine_end.date().isoformat(),
                "UNITS": int(routine_units),
                "LINE_ALLOWED_AMT": round(allowed * weights[line_num - 1], 2),
                "LINE_PAID_AMT": round(paid * weights[line_num - 1], 2),
            })
            line_num += 1

            if icu_days > 0:
                icu_start = seg_start + pd.Timedelta(days=int(rng.integers(0, max(1, seg_days - icu_days + 1))))
                icu_end = icu_start + pd.Timedelta(days=icu_days)
                if rng.random() < cfg.p_line_date_out_of_bounds:
                    icu_start = max(seg_start - pd.Timedelta(days=1), icu_start)

                line_rows.append({
                    "CLM_ID": clm_id,
                    "LINE_NUM": line_num,
                    "REV_CNTR_CD": rng.choice(rev_icu),
                    "LINE_SRVC_BGN_DT": icu_start.date().isoformat(),
                    "LINE_SRVC_END_DT": icu_end.date().isoformat(),
                    "UNITS": int(icu_days),
                    "LINE_ALLOWED_AMT": round(allowed * weights[line_num - 1], 2),
                    "LINE_PAID_AMT": round(paid * weights[line_num - 1], 2),
                })
                line_num += 1

            other_pool = rev_pharm + rev_lab + rev_er + rev_other
            other_revs = rng.choice(other_pool, size=n_other_lines, replace=True)
            for k in range(n_other_lines):
                bgn = seg_start + pd.Timedelta(days=int(rng.integers(0, max(1, seg_days + 1))))
                end = min(seg_end, bgn + pd.Timedelta(days=int(rng.integers(0, 2))))
                units = int(rng.integers(1, 6))
                if rng.random() < cfg.p_line_date_out_of_bounds:
                    end = end + pd.Timedelta(days=1)

                line_rows.append({
                    "CLM_ID": clm_id,
                    "LINE_NUM": line_num,
                    "REV_CNTR_CD": str(other_revs[k]),
                    "LINE_SRVC_BGN_DT": bgn.date().isoformat(),
                    "LINE_SRVC_END_DT": end.date().isoformat(),
                    "UNITS": units,
                    "LINE_ALLOWED_AMT": round(allowed * weights[line_num - 1], 2),
                    "LINE_PAID_AMT": round(paid * weights[line_num - 1], 2),
                })
                line_num += 1

            if rng.random() < cfg.p_duplicate_header_row:
                header_rows.append(header_rows[-1].copy())
            if rng.random() < cfg.p_duplicate_line_row and len(line_rows) > 0:
                line_rows.append(line_rows[-1].copy())

    ip_header_messy = pd.DataFrame(header_rows)
    ip_line_messy = pd.DataFrame(line_rows)

    ip_header_messy = ip_header_messy.sample(frac=1, random_state=cfg.seed).reset_index(drop=True)
    ip_line_messy = ip_line_messy.sample(frac=1, random_state=cfg.seed).reset_index(drop=True)

    return xwalk, true_stays, ip_header_messy, ip_line_messy


def build_fact_inpatient_stay(ip_header_messy: pd.DataFrame, ip_line_messy: pd.DataFrame, max_expand_days_per_line: int = 31):
    hdr = ip_header_messy.copy()
    hdr["PAID_AMT"] = pd.to_numeric(hdr["PAID_AMT"], errors="coerce").fillna(0.0)
    hdr["ALLOWED_AMT"] = pd.to_numeric(hdr["ALLOWED_AMT"], errors="coerce").fillna(0.0)

    hdr = hdr.sort_values(["CLM_ID", "PAID_AMT", "ALLOWED_AMT"], ascending=[True, False, False])
    hdr = hdr.drop_duplicates(subset=["CLM_ID"], keep="first").reset_index(drop=True)

    hdr["ADMIT_DT_TS"] = safe_ts(hdr["ADMIT_DT"])
    hdr["DISCH_DT_TS"] = safe_ts(hdr["DISCH_DT"])
    hdr = hdr.sort_values(["MSIS_ID", "ADMIT_DT_TS", "DISCH_DT_TS", "CLM_ID"]).reset_index(drop=True)

    prior_disch = hdr.groupby("MSIS_ID")["DISCH_DT_TS"].shift(1)
    new_stay = (prior_disch.isna()) | (hdr["ADMIT_DT_TS"] > (prior_disch + pd.Timedelta(days=1)))
    hdr["stay_seq"] = new_stay.groupby(hdr["MSIS_ID"]).cumsum()
    # Deterministic, collision-free stay id
    hdr = hdr.sort_values(["MSIS_ID", "stay_seq"]).reset_index(drop=True)

    hdr["stay_id"] = (
        "STAY_" +
        hdr["MSIS_ID"] +
        "_" +
        hdr["stay_seq"].astype(str)
    )

    
    def mode_nonnull(s: pd.Series):
        s2 = s.dropna()
        if len(s2) == 0:
            return None
        return s2.value_counts().index[0]

    stay_agg = hdr.groupby(["stay_id", "MSIS_ID", "SUBMTG_STATE_CD", "STATE_CD"], as_index=False).agg(
        admit_dt=("ADMIT_DT_TS", "min"),
        discharge_dt=("DISCH_DT_TS", "max"),
        total_paid=("PAID_AMT", "sum"),
        total_allowed=("ALLOWED_AMT", "sum"),
        drg_cd=("DRG_CD", mode_nonnull),
        dx_cd_1=("DX_CD_1", mode_nonnull),
        any_denied=("DENIED_IND", lambda x: int(pd.to_numeric(x, errors="coerce").fillna(0).max() > 0)),
        n_claims=("CLM_ID", "nunique"),
    )
    stay_agg["length_of_stay"] = (stay_agg["discharge_dt"] - stay_agg["admit_dt"]).dt.days.clip(lower=0)

    lines = ip_line_messy.copy()
    lines = lines.drop_duplicates(subset=["CLM_ID", "LINE_NUM", "REV_CNTR_CD", "LINE_SRVC_BGN_DT", "LINE_SRVC_END_DT"])
    lines["LINE_SRVC_BGN_TS"] = safe_ts(lines["LINE_SRVC_BGN_DT"])
    lines["LINE_SRVC_END_TS"] = safe_ts(lines["LINE_SRVC_END_DT"])

    clm_to_stay = hdr[["CLM_ID", "stay_id", "MSIS_ID"]].drop_duplicates()
    lines = lines.merge(clm_to_stay, on="CLM_ID", how="left")

    lines["is_icu"] = lines["REV_CNTR_CD"].astype(str).str.startswith("020")
    icu_lines = lines.loc[lines["is_icu"] & lines["stay_id"].notna(), ["stay_id", "LINE_SRVC_BGN_TS", "LINE_SRVC_END_TS"]].copy()

    if len(icu_lines) > 0:
        def expand_dates(b, e):
            if pd.isna(b) or pd.isna(e):
                return []
            if e < b:
                b, e = e, b
            days = pd.date_range(b.normalize(), e.normalize(), freq="D")
            return list(days[:max_expand_days_per_line])

        icu_lines["icu_dates"] = [expand_dates(b, e) for b, e in zip(icu_lines["LINE_SRVC_BGN_TS"], icu_lines["LINE_SRVC_END_TS"])]
        icu_exploded = icu_lines[["stay_id", "icu_dates"]].explode("icu_dates").dropna()
        icu_days = icu_exploded.groupby("stay_id")["icu_dates"].nunique().reset_index(name="icu_days")
    else:
        icu_days = pd.DataFrame({"stay_id": [], "icu_days": []})

    fact = stay_agg.merge(icu_days, on="stay_id", how="left")
    fact["icu_days"] = fact["icu_days"].fillna(0).astype(int)

    fact["admit_year"] = fact["admit_dt"].dt.year
    fact["admit_quarter"] = ((fact["admit_dt"].dt.month - 1) // 3 + 1).astype(int)
    fact["qtr_start"] = fact.apply(lambda r: quarter_start(int(r["admit_year"]), int(r["admit_quarter"])).date().isoformat(), axis=1)

    fact["admit_dt"] = fact["admit_dt"].dt.date.astype(str)
    fact["discharge_dt"] = fact["discharge_dt"].dt.date.astype(str)
    fact["total_paid"] = fact["total_paid"].round(2)
    fact["total_allowed"] = fact["total_allowed"].round(2)

    fact = fact[[
        "stay_id",
        "MSIS_ID",
        "SUBMTG_STATE_CD",
        "STATE_CD",
        "admit_dt",
        "discharge_dt",
        "length_of_stay",
        "icu_days",
        "drg_cd",
        "dx_cd_1",
        "total_paid",
        "total_allowed",
        "any_denied",
        "n_claims",
        "admit_year",
        "admit_quarter",
        "qtr_start",
    ]].sort_values(["MSIS_ID", "admit_dt", "stay_id"]).reset_index(drop=True)

    return fact


if __name__ == "__main__":
    cfg = InpatientMessyConfig(
        n_people=10_000,
        start_month="2022-01-01",
        n_months=24,
        seed=7,
        p_any_stay_per_person=0.25,
        min_stays=1,
        max_stays=2,
    )

    xwalk, true_stays, ip_header_messy, ip_line_messy = generate_messy_inpatient_claims(cfg)
    fact_inpatient_stay = build_fact_inpatient_stay(
        ip_header_messy,
        ip_line_messy,
        max_expand_days_per_line=cfg.max_expand_days_per_line
    )

    xwalk.to_csv("output/person_msis_xwalk.csv", index=False)
    true_stays.to_csv("output/true_stays_reference.csv", index=False)
    ip_header_messy.to_csv("output/ip_claim_header_messy.csv", index=False)
    ip_line_messy.to_csv("output/ip_claim_line_messy.csv", index=False)
    fact_inpatient_stay.to_csv("output/fact_inpatient_stay.csv", index=False)

    print("Row counts",
          {
              "xwalk": len(xwalk),
              "true_stays": len(true_stays),
              "ip_header_messy": len(ip_header_messy),
              "ip_line_messy": len(ip_line_messy),
              "fact_inpatient_stay": len(fact_inpatient_stay),
          })
