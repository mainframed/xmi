# Manual z/OS Test — XMI Embedded Message

Verify that a message created by `createxmi` displays correctly on a z/OS terminal
when receiving the XMI via TSO `RECEIVE`.

---

## Automated smoke test — `tests/zos_smoke.py`

The script `tests/zos_smoke.py` builds all XMI variants, verifies them
with the local parser, and optionally uploads them to z/OS via FTP.

| Dataset suffix | Content | Has message | Format |
|---|---|---|---|
| `XMI1M`  | Sequential file                       | Yes | 80×32  |
| `XMI1`   | Sequential file                       | No  | —      |
| `XMIPM`  | PDS (2 members)                       | Yes | 80×32  |
| `XMIP`   | PDS (2 members)                       | No  | —      |
| `XMI1W`  | Sequential file                       | Yes | 132×27 |
| `XMIPNM` | Nested PDS (text member + inner XMI)  | Yes | 80×32  |
| `XMIPN`  | Nested PDS (text member + inner XMI)  | No  | —      |

The nested variants (`XMIPNM`, `XMIPN`) test that a PDS member which is itself
an XMI file can be received from z/OS after the outer PDS is received.  This
exercises the binary-member LRECL padding fix.

### Prerequisites — allocate target datasets on z/OS

Before uploading, the 4 target datasets must exist on z/OS.  z/OS FTP
can auto-allocate them from the `SITE` parameters, but pre-allocating
avoids permission and space surprises.

**Option A — TSO ALLOCATE** (ISPF option 6 or native TSO, one command per dataset):

```
ALLOC DA('IBMUSER.XMILIB.XMI1M')  NEW CATALOG SPACE(1,1) TRACKS RECFM(F,B) LRECL(80) BLKSIZE(3120) UNIT(SYSDA)
ALLOC DA('IBMUSER.XMILIB.XMI1')   NEW CATALOG SPACE(1,1) TRACKS RECFM(F,B) LRECL(80) BLKSIZE(3120) UNIT(SYSDA)
ALLOC DA('IBMUSER.XMILIB.XMIPM')  NEW CATALOG SPACE(1,1) TRACKS RECFM(F,B) LRECL(80) BLKSIZE(3120) UNIT(SYSDA)
ALLOC DA('IBMUSER.XMILIB.XMIP')   NEW CATALOG SPACE(1,1) TRACKS RECFM(F,B) LRECL(80) BLKSIZE(3120) UNIT(SYSDA)
ALLOC DA('IBMUSER.XMILIB.XMI1W')  NEW CATALOG SPACE(1,1) TRACKS RECFM(F,B) LRECL(80) BLKSIZE(3120) UNIT(SYSDA)
ALLOC DA('IBMUSER.XMILIB.XMIPNM') NEW CATALOG SPACE(1,1) TRACKS RECFM(F,B) LRECL(80) BLKSIZE(3120) UNIT(SYSDA)
ALLOC DA('IBMUSER.XMILIB.XMIPN')  NEW CATALOG SPACE(1,1) TRACKS RECFM(F,B) LRECL(80) BLKSIZE(3120) UNIT(SYSDA)
```

**Option B — JCL** (submit as a batch job):

```jcl
//XMIALLOC JOB ,'ALLOCATE XMILIB',CLASS=A,MSGCLASS=X
//STEP1    EXEC PGM=IEFBR14
//XMI1M    DD DSN=IBMUSER.XMILIB.XMI1M,
//            DISP=(NEW,CATLG,DELETE),
//            SPACE=(TRK,(1,1)),
//            DCB=(RECFM=FB,LRECL=80,BLKSIZE=3120),
//            UNIT=SYSDA
//XMI1     DD DSN=IBMUSER.XMILIB.XMI1,
//            DISP=(NEW,CATLG,DELETE),
//            SPACE=(TRK,(1,1)),
//            DCB=(RECFM=FB,LRECL=80,BLKSIZE=3120),
//            UNIT=SYSDA
//XMIPM    DD DSN=IBMUSER.XMILIB.XMIPM,
//            DISP=(NEW,CATLG,DELETE),
//            SPACE=(TRK,(1,1)),
//            DCB=(RECFM=FB,LRECL=80,BLKSIZE=3120),
//            UNIT=SYSDA
//XMIP     DD DSN=IBMUSER.XMILIB.XMIP,
//            DISP=(NEW,CATLG,DELETE),
//            SPACE=(TRK,(1,1)),
//            DCB=(RECFM=FB,LRECL=80,BLKSIZE=3120),
//            UNIT=SYSDA
//XMI1W    DD DSN=IBMUSER.XMILIB.XMI1W,
//            DISP=(NEW,CATLG,DELETE),
//            SPACE=(TRK,(1,1)),
//            DCB=(RECFM=FB,LRECL=80,BLKSIZE=3120),
//            UNIT=SYSDA
//XMIPNM   DD DSN=IBMUSER.XMILIB.XMIPNM,
//            DISP=(NEW,CATLG,DELETE),
//            SPACE=(TRK,(1,1)),
//            DCB=(RECFM=FB,LRECL=80,BLKSIZE=3120),
//            UNIT=SYSDA
//XMIPN    DD DSN=IBMUSER.XMILIB.XMIPN,
//            DISP=(NEW,CATLG,DELETE),
//            SPACE=(TRK,(1,1)),
//            DCB=(RECFM=FB,LRECL=80,BLKSIZE=3120),
//            UNIT=SYSDA
//
```

**Option C — ISPF 3.2 (Data Set Utility)**

For each of the four names above, enter option `A` (Allocate), then fill in:
- Data set name: `IBMUSER.XMILIB.XMI1M` (etc.)
- Allocation units: `TRK`, Primary: `1`, Secondary: `1`
- Record format: `FB`, LRECL: `80`, Block size: `3120`

> **Re-running:** if the datasets already exist from a previous run, delete
> them first with `DELETE 'IBMUSER.XMILIB.XMI1M'` (etc.) in TSO, or use
> ISPF 3.2 option `D`.

### Step 1 — local build and parse (no z/OS needed)

```bash
python tests/zos_smoke.py
```

Expected output:

```
Building XMI files...
  XMI1M    817 bytes
  XMI1     464 bytes
  XMIPM  1,670 bytes
  XMIP   1,317 bytes

Local parser verification:
  [PASS] XMI1M   has_message=True   message: 'XMI smoke test\nCreated by Python...'
  [PASS] XMI1    has_message=False
  [PASS] XMIPM   has_message=True   message: 'XMI smoke test\nCreated by Python...'
  [PASS] XMIP    has_message=False

All local checks passed.
```

All four `[PASS]` lines must appear before proceeding to upload.

### Step 2 — upload to z/OS

```bash
# Default host (192.168.1.141) and HLQ (IBMUSER.XMILIB)
python tests/zos_smoke.py --upload

# Override host or HLQ
python tests/zos_smoke.py --upload --host 10.0.0.1 --hlq MYUID.XMILIB
```

The script prompts for a username (default `IBMUSER`) and password, then
uploads the 4 files via FTP in binary mode with `RECFM=FB LRECL=80 BLKSIZE=3120`.

### Step 3 — RECEIVE on z/OS

From TSO (ISPF option 6 or native), run each RECEIVE in turn:

```
RECEIVE INDSN('IBMUSER.XMILIB.XMI1M')
RECEIVE INDSN('IBMUSER.XMILIB.XMI1')
RECEIVE INDSN('IBMUSER.XMILIB.XMIPM')
RECEIVE INDSN('IBMUSER.XMILIB.XMIP')
```

When prompted (`INMR906A Enter restore parameters`), supply a target, e.g.:

```
DA('IBMUSER.RECEIVED.TEST') UNIT(SYSDA) SPACE(1,1) TRACKS
```

### Step 3 — expected results

| Dataset | Expected behaviour |
|---|---|
| `XMI1M`  | Message text appears on terminal (80 cols), then sequential dataset restores |
| `XMI1`   | No message — prompts immediately for restore parameters, dataset restores |
| `XMIPM`  | Message text appears on terminal (80 cols), then PDS restores with members MEMBER1, MEMBER2 |
| `XMIP`   | No message — PDS restores with members MEMBER1, MEMBER2 |
| `XMI1W`  | Wide message fills 132 columns on terminal, then sequential dataset restores |
| `XMIPNM` | Message displays, PDS restores with members INNERXMI and README |
| `XMIPN`  | No message, PDS restores with members INNERXMI and README |

For the nested variants, after receiving the outer PDS run a second RECEIVE from
the inner member to verify the binary member survived intact:

```
RECEIVE INDSN('IBMUSER.RECEIVED.XMIPN(INNERXMI)')
```

This should restore a PDS with members INNER1 and INNER2.

Verification checklist:
- [ ] `XMI1M`  — message displays, sequential dataset restored successfully
- [ ] `XMI1`   — no message, sequential dataset restored successfully
- [ ] `XMIPM`  — message displays, PDS restored with 2 members (MEMBER1, MEMBER2)
- [ ] `XMIP`   — no message, PDS restored with 2 members (MEMBER1, MEMBER2)
- [ ] `XMI1W`  — wide message fills 132 columns (verify on Model 5 terminal), sequential dataset restored
- [ ] `XMIPNM` — message displays, outer PDS has INNERXMI + README; inner RECEIVE restores INNER1, INNER2
- [ ] `XMIPN`  — no message, outer PDS has INNERXMI + README; inner RECEIVE restores INNER1, INNER2

---

## Manual tests — individual XMI variants

The sections below cover targeted manual tests for specific format options.
Use these when you want to test a specific scenario in isolation rather than
running the full smoke test above.

---

## Test A — Standard format (80×32, Model 3 terminal)

### 1. Create a test XMI with a message

```bash
# Create a simple dataset source
echo "This is a test dataset." > /tmp/test_dataset.txt

# Create an 80x32 message file
cat > /tmp/banner-80x32.txt << 'EOF'
========================================
  Hello from Python xmi library!
  Format: 80x32 (80 columns, 32 lines)
========================================
EOF

# Build the XMI
createxmi /tmp/test_dataset.txt \
  -o /tmp/TEST80.XMI \
  --message-file /tmp/banner-80x32.txt \
  --message-format 80x32
```

### 2. Verify locally (optional sanity check)

```bash
python -c "
import xmi
x = xmi.XMIT(filename='/tmp/TEST80.XMI', quiet=True)
x.open()
print('has_message:', x.has_message())
print(x.get_message())
"
```

### 3. Upload to z/OS via FTP (binary / TYPE I)

```
ftp your.zos.host
binary
site recfm=fb lrecl=80 blksize=3200
put /tmp/TEST80.XMI 'YOUR.USERID.TEST80.XMI'
quit
```

> **Binary mode is required.** XMI files are EBCDIC binary with internal
> NETDATA segmentation. ASCII↔EBCDIC translation during FTP will corrupt the file.

### 4. Receive on z/OS via TSO

In a TSO session (ISPF option 6 or native TSO):

```
RECEIVE INDSN('YOUR.USERID.TEST80.XMI')
```

### 5. Expected result

The message lines appear on the terminal *before* the dataset is received:

```
========================================
  Hello from Python xmi library!
  Format: 80x32 (80 columns, 32 lines)
========================================
```

Then z/OS prompts for the target dataset name as usual.

Verify:
- [ ] Message displays before dataset is received
- [ ] All lines fit within 80 columns — no truncation or wrap
- [ ] Characters render correctly — no garbage/question marks
- [ ] Dataset is received successfully after the message

---

## Test B — Wide format (132×27, Model 5 terminal)

### 1. Create a 132-column XMI

```bash
cat > /tmp/banner-132x27.txt << 'EOF'
====================================================================================================================================
  Hello from Python xmi library — wide format!                 132 columns x 27 lines — Model 5 terminal
====================================================================================================================================
EOF

createxmi /tmp/test_dataset.txt \
  -o /tmp/TEST132.XMI \
  --message-file /tmp/banner-132x27.txt \
  --message-format 132x27
```

### 2. Upload and receive (same FTP steps as Test A)

```
ftp your.zos.host
binary
site recfm=fb lrecl=132 blksize=3300
put /tmp/TEST132.XMI 'YOUR.USERID.TEST132.XMI'
quit
```

```
RECEIVE INDSN('YOUR.USERID.TEST132.XMI')
```

Verify on a **132-column (Model 5) terminal session**:
- [ ] Message displays before dataset is received
- [ ] Lines fill the full 132-column width without wrapping
- [ ] Characters render correctly
- [ ] Dataset is received successfully

---

## Test C — Inline message via CLI

```bash
createxmi /tmp/test_dataset.txt \
  -o /tmp/TESTINLINE.XMI \
  --message "Hello from Python!\nThis is line two."
```

Upload and receive as in Test A. Verify two message lines display.

---

## Notes

- If the message displays as garbage characters, check that the FTP transfer was binary (`TYPE I`).
- If lines wrap unexpectedly, verify your terminal is set to 132 columns for the 132x27 test.
- Future: USS folder workflow — drop the XMI into `/u/yourid/` and `RECEIVE` from USS path.
