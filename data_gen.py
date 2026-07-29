"""
Synthetic healthcare claims dataset generator.

Builds five related tables (members, providers, physicians, diagnoses,
procedures, claims) as pandas DataFrames and registers them in a DuckDB
connection so they can be queried with plain SQL. Used by
claims_sql_playground.ipynb, but can also be run standalone:

    python data_gen.py

which prints row counts for a quick sanity check.
"""

import random
from datetime import date, timedelta

import duckdb
import pandas as pd
from faker import Faker

SEED = 42
random.seed(SEED)
fake = Faker()
Faker.seed(SEED)

US_STATES = ["CA", "TX", "NY", "FL", "IL", "PA", "OH", "GA", "NC", "MI"]
PLAN_TYPES = ["HMO", "PPO", "EPO", "POS"]
PROVIDER_TYPES = ["Hospital", "Clinic", "Ambulatory Surgical Center", "Independent Practice", "Pharmacy"]
SPECIALTIES = [
    "Internal Medicine", "Cardiology", "Orthopedics", "Pediatrics",
    "Family Medicine", "General Surgery", "Dermatology", "Psychiatry",
    "Emergency Medicine", "Radiology",
]
CLAIM_TYPES = ["Inpatient", "Outpatient", "Professional", "Pharmacy"]
CLAIM_STATUSES = ["Paid", "Denied", "Pending", "Partially Paid"]
DENIAL_REASONS = [
    "Not medically necessary", "Duplicate claim", "Out of network",
    "Prior authorization required", "Coverage terminated", "Missing documentation",
]

DIAGNOSES = [
    ("E11.9", "Type 2 diabetes mellitus without complications", "Endocrine"),
    ("I10", "Essential (primary) hypertension", "Cardiovascular"),
    ("J45.909", "Unspecified asthma, uncomplicated", "Respiratory"),
    ("M54.5", "Low back pain", "Musculoskeletal"),
    ("K21.9", "Gastro-esophageal reflux disease without esophagitis", "Digestive"),
    ("F41.1", "Generalized anxiety disorder", "Mental Health"),
    ("N39.0", "Urinary tract infection, site not specified", "Genitourinary"),
    ("S52.501A", "Fracture of lower end of radius, initial encounter", "Injury"),
    ("I25.10", "Atherosclerotic heart disease of native coronary artery", "Cardiovascular"),
    ("J06.9", "Acute upper respiratory infection, unspecified", "Respiratory"),
    ("E78.5", "Hyperlipidemia, unspecified", "Endocrine"),
    ("M17.9", "Osteoarthritis of knee, unspecified", "Musculoskeletal"),
    ("R51", "Headache", "Symptom"),
    ("Z00.00", "General adult medical examination", "Preventive"),
    ("O80", "Encounter for full-term uncomplicated delivery", "Maternity"),
]

PROCEDURES = [
    ("99213", "Office/outpatient visit, established patient, low complexity", "Evaluation & Management"),
    ("99214", "Office/outpatient visit, established patient, moderate complexity", "Evaluation & Management"),
    ("99283", "Emergency department visit, moderate severity", "Evaluation & Management"),
    ("70450", "CT scan of head/brain without contrast", "Radiology"),
    ("71046", "Chest X-ray, 2 views", "Radiology"),
    ("80053", "Comprehensive metabolic panel", "Laboratory"),
    ("85025", "Complete blood count with differential", "Laboratory"),
    ("93000", "Electrocardiogram, routine", "Cardiology"),
    ("27447", "Total knee replacement", "Surgery"),
    ("29881", "Knee arthroscopy with meniscectomy", "Surgery"),
    ("47562", "Laparoscopic cholecystectomy", "Surgery"),
    ("90834", "Psychotherapy, 45 minutes", "Behavioral Health"),
    ("J1885", "Injection, ketorolac tromethamine", "Drug/Injection"),
    ("99396", "Periodic preventive exam, established patient, 40-64 years", "Preventive"),
    ("59400", "Routine obstetric care, vaginal delivery", "Maternity"),
]


def _random_date(start: date, end: date) -> date:
    delta_days = (end - start).days
    return start + timedelta(days=random.randint(0, delta_days))


def generate_providers(n=8) -> pd.DataFrame:
    rows = []
    for i in range(1, n + 1):
        rows.append({
            "provider_id": i,
            "provider_name": fake.company() + (" Medical Center" if random.random() < 0.5 else " Health Group"),
            "provider_type": random.choice(PROVIDER_TYPES),
            "npi": fake.unique.numerify("##########"),
            "state": random.choice(US_STATES),
            "in_network": random.random() < 0.85,
        })
    return pd.DataFrame(rows)


def generate_physicians(providers_df: pd.DataFrame, n=20) -> pd.DataFrame:
    rows = []
    for i in range(1, n + 1):
        rows.append({
            "physician_id": i,
            "first_name": fake.first_name(),
            "last_name": fake.last_name(),
            "specialty": random.choice(SPECIALTIES),
            "npi": fake.unique.numerify("##########"),
            "provider_id": int(random.choice(providers_df["provider_id"])),
            "years_experience": random.randint(1, 35),
            "supervisor_id": None,
        })
    df = pd.DataFrame(rows)

    # Build a shallow reporting hierarchy: ~4 department heads (no supervisor),
    # everyone else reports to a department head with the same specialty when
    # possible, otherwise to a random head. Useful for a recursive-CTE demo.
    heads = df.sample(n=4, random_state=SEED)["physician_id"].tolist()
    for idx, row in df.iterrows():
        if row["physician_id"] in heads:
            continue
        same_specialty_heads = df[df["physician_id"].isin(heads) & (df["specialty"] == row["specialty"])]
        chosen = (
            same_specialty_heads.iloc[0]["physician_id"]
            if len(same_specialty_heads) > 0
            else random.choice(heads)
        )
        df.at[idx, "supervisor_id"] = int(chosen)
    df["supervisor_id"] = df["supervisor_id"].astype("Int64")
    return df


def generate_diagnoses() -> pd.DataFrame:
    return pd.DataFrame(DIAGNOSES, columns=["diagnosis_code", "diagnosis_description", "category"])


def generate_procedures() -> pd.DataFrame:
    return pd.DataFrame(PROCEDURES, columns=["procedure_code", "procedure_description", "category"])


def generate_members(n=150) -> pd.DataFrame:
    rows = []
    for i in range(1, n + 1):
        gender = random.choice(["M", "F"])
        first = fake.first_name_male() if gender == "M" else fake.first_name_female()
        enrollment = _random_date(date(2022, 1, 1), date(2024, 6, 1))
        rows.append({
            "member_id": i,
            "first_name": first,
            "last_name": fake.last_name(),
            "dob": fake.date_of_birth(minimum_age=1, maximum_age=90),
            "gender": gender,
            "state": random.choice(US_STATES),
            "plan_type": random.choice(PLAN_TYPES),
            "enrollment_date": enrollment,
            "member_status": "Active" if random.random() < 0.9 else "Inactive",
        })
    return pd.DataFrame(rows)


def generate_claims(members_df, providers_df, physicians_df, diagnoses_df, procedures_df, n=1000) -> pd.DataFrame:
    rows = []
    start, end = date(2023, 1, 1), date(2024, 6, 30)
    physician_records = physicians_df.to_dict("records")
    for i in range(1, n + 1):
        member_id = int(random.choice(members_df["member_id"]))
        physician = random.choice(physician_records)
        physician_id = int(physician["physician_id"])
        provider_id = int(physician["provider_id"])
        diagnosis_code = random.choice(diagnoses_df["diagnosis_code"].tolist())
        procedure_code = random.choice(procedures_df["procedure_code"].tolist())

        service_date = _random_date(start, end)
        submitted_date = service_date + timedelta(days=random.randint(0, 21))

        # Right-skewed like real claim amounts (lots of small/medium claims,
        # a long tail of expensive ones), with a small fraction of claims
        # deliberately seeded as extreme outliers relative to their own
        # provider's typical amount -- gives the fraud-detection queries in
        # the notebook genuine signal to find, the way real claims data
        # (mostly normal, a few truly anomalous) does.
        base_amount = random.lognormvariate(7.0, 0.8)
        if random.random() < 0.015:
            base_amount *= random.uniform(4, 7)
        claim_amount = round(min(max(base_amount, 60), 60000), 2)

        status = random.choices(CLAIM_STATUSES, weights=[0.65, 0.12, 0.10, 0.13])[0]

        if status == "Denied":
            allowed_amount = 0.0
            paid_amount = 0.0
            denial_reason = random.choice(DENIAL_REASONS)
        elif status == "Pending":
            allowed_amount = None
            paid_amount = None
            denial_reason = None
        else:
            allowed_amount = round(claim_amount * random.uniform(0.6, 0.95), 2)
            if status == "Partially Paid":
                paid_amount = round(allowed_amount * random.uniform(0.3, 0.85), 2)
            else:
                paid_amount = allowed_amount
            denial_reason = None

        rows.append({
            "claim_id": i,
            "member_id": member_id,
            "provider_id": provider_id,
            "physician_id": physician_id,
            "diagnosis_code": diagnosis_code,
            "procedure_code": procedure_code,
            "claim_type": random.choice(CLAIM_TYPES),
            "service_date": service_date,
            "submitted_date": submitted_date,
            "claim_amount": claim_amount,
            "allowed_amount": allowed_amount,
            "paid_amount": paid_amount,
            "claim_status": status,
            "denial_reason": denial_reason,
        })
    return pd.DataFrame(rows)


def generate_claim_lines(claims_df: pd.DataFrame, procedures_df: pd.DataFrame) -> pd.DataFrame:
    """One-to-many child of claims: each claim has 1-4 line items whose charges
    sum to roughly the claim header's claim_amount. Exists specifically so the
    notebook can demonstrate the classic "fan-out" join bug, where joining a
    one-to-many child table to a parent and re-summing a parent-level column
    silently inflates totals.
    """
    rows = []
    line_id = 1
    proc_codes = procedures_df["procedure_code"].tolist()
    for claim in claims_df.itertuples(index=False):
        n_lines = random.randint(1, 4)
        # split the header claim_amount across n_lines random weights
        weights = [random.uniform(0.2, 1.0) for _ in range(n_lines)]
        total_weight = sum(weights)
        for line_number, weight in enumerate(weights, start=1):
            rows.append({
                "claim_line_id": line_id,
                "claim_id": claim.claim_id,
                "line_number": line_number,
                "procedure_code": random.choice(proc_codes),
                "line_charge_amount": round(claim.claim_amount * weight / total_weight, 2),
            })
            line_id += 1
    return pd.DataFrame(rows)


def build_dataset():
    providers_df = generate_providers()
    physicians_df = generate_physicians(providers_df)
    diagnoses_df = generate_diagnoses()
    procedures_df = generate_procedures()
    members_df = generate_members()
    claims_df = generate_claims(members_df, providers_df, physicians_df, diagnoses_df, procedures_df)
    claim_lines_df = generate_claim_lines(claims_df, procedures_df)
    return {
        "providers": providers_df,
        "physicians": physicians_df,
        "diagnoses": diagnoses_df,
        "procedures": procedures_df,
        "members": members_df,
        "claims": claims_df,
        "claim_lines": claim_lines_df,
    }


def load_into_duckdb(con: duckdb.DuckDBPyConnection) -> dict:
    """Generate the dataset and register every table on the given connection."""
    tables = build_dataset()
    for name, df in tables.items():
        con.register(name, df)
    return tables


if __name__ == "__main__":
    tables = build_dataset()
    for name, df in tables.items():
        print(f"{name:12s} {len(df):5d} rows")
