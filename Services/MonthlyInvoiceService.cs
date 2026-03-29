using System.Data;
using APWS.Models;
using APWS.Models.MonthlyInvoice;
using Microsoft.Data.SqlClient;

namespace APWS.Services;

public sealed class MonthlyInvoiceService
{
    private readonly string _connectionString;

    private decimal _totalRentAmount;
    private decimal _taxOfRentAmount;
    private int _numOfCoolers;
    private decimal _amountPerCooler;
    private string _pricePackage = string.Empty;
    private string _paymentType = string.Empty;
    private string _discount = string.Empty;
    private string _taxType = string.Empty;
    private decimal _bottlePrice;
    private bool _svat;
    private DateTime _preparedDate;

    public MonthlyInvoiceService(IConfiguration configuration)
    {
        _connectionString = configuration.GetConnectionString("APWSDB")
            ?? throw new InvalidOperationException("Connection string 'APWSDB' is not configured.");
    }

    public async Task<IReadOnlyList<OptionItem>> GetMonthsAsync(CancellationToken cancellationToken = default)
    {
        var options = await FillDataAsync("Months", 2, cancellationToken);
        if (options.Count > 0)
        {
            return options;
        }

        return Enumerable.Range(1, 12)
            .Select(m => new OptionItem(m.ToString(), new DateTime(2000, m, 1).ToString("MMMM")))
            .ToList();
    }

    public async Task<IReadOnlyList<OptionItem>> GetExecutivesAsync(CancellationToken cancellationToken = default)
        => await FillDataAsync("ExeNamesforinvo", 1, cancellationToken);

    public async Task<IReadOnlyList<OptionItem>> GetBranchesAsync(string customerCode, CancellationToken cancellationToken = default)
    {
        var output = new List<OptionItem>();
        if (string.IsNullOrWhiteSpace(customerCode))
        {
            return output;
        }

        const string sql = @"
SELECT BranchCode, BranchCode
FROM tbl_Branch
WHERE CustomerCode = @cust
ORDER BY BranchCode";

        await using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken);
        await using var cmd = new SqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("@cust", customerCode.Trim());

        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            var val = reader.IsDBNull(0) ? string.Empty : reader.GetValue(0)?.ToString() ?? string.Empty;
            var text = reader.IsDBNull(1) ? val : reader.GetValue(1)?.ToString() ?? val;
            if (!string.IsNullOrWhiteSpace(val))
            {
                output.Add(new OptionItem(val, text));
            }
        }

        return output;
    }

    public async Task<InvoiceRunResult> GenerateAllAsync(
        int year,
        int month,
        DateTime preparedDate,
        bool coolerOnly,
        Action<string>? log,
        CancellationToken cancellationToken = default)
    {
        _preparedDate = preparedDate;
        var total = new InvoiceRunResult();

        if (coolerOnly)
        {
            log?.Invoke("Starting Cooler-Only Invoice Generation...");
            var cooler = await GenerateCoolerOnlyInvoicesAsync(year, month, log, cancellationToken);
            Merge(total, cooler);
            log?.Invoke("Cooler-Only Invoice Generation Complete");
            return total;
        }

        log?.Invoke("Starting Cooler-Only Invoice Generation...");
        var coolerFirst = await GenerateCoolerOnlyInvoicesAsync(year, month, log, cancellationToken);
        Merge(total, coolerFirst);
        log?.Invoke("Cooler-Only Invoice Generation Complete");

        log?.Invoke("Starting Executive-Wise Invoice Generation...");
        var exec = await GenerateExecutiveWiseInvoicesAsync(year, month, log, cancellationToken);
        Merge(total, exec);
        log?.Invoke("Executive-Wise Invoice Generation Complete");

        return total;
    }

    public async Task<InvoiceRunResult> GenerateAllAsync(
        GenerateAllMonthlyInvoiceModel request,
        Action<string>? log,
        CancellationToken cancellationToken = default)
        => await GenerateAllAsync(
            request.Year,
            request.Month,
            request.PreparedDate,
            request.CoolerOnly,
            log,
            cancellationToken);

    public async Task<InvoiceRunResult> GenerateSingleAsync(
        string customerCode,
        string branchCode,
        int year,
        int month,
        DateTime preparedDate,
        Action<string>? log,
        CancellationToken cancellationToken = default)
    {
        _preparedDate = preparedDate;

        var result = new InvoiceRunResult
        {
            Processed = 1
        };

        log?.Invoke($"Processing individual invoice: {customerCode}-{branchCode}");

        if (!await InvoiceAlreadyExistsAsync(customerCode, branchCode, month, year, cancellationToken))
        {
            var inserted = await Task.Run(() => GenInvoice(customerCode, branchCode, month, year), cancellationToken);
            if (inserted)
            {
                result.Inserted = 1;
                log?.Invoke($"Done Single: {customerCode}-{branchCode}");
            }
            else
            {
                result.Skipped = 1;
                log?.Invoke($"Skipped Single (No amount/error): {customerCode}-{branchCode}");
            }
        }
        else
        {
            result.Skipped = 1;
            log?.Invoke($"Skipped (Exists): {customerCode}-{branchCode}");
        }

        return result;
    }

    public async Task<InvoiceRunResult> GenerateSingleAsync(
        GenerateSingleMonthlyInvoiceModel request,
        Action<string>? log,
        CancellationToken cancellationToken = default)
        => await GenerateSingleAsync(
            request.CustomerCode,
            request.BranchCode,
            request.Year,
            request.Month,
            request.PreparedDate,
            log,
            cancellationToken);

    private static void Merge(InvoiceRunResult into, InvoiceRunResult from)
    {
        into.Processed += from.Processed;
        into.Inserted += from.Inserted;
        into.Skipped += from.Skipped;
    }

    private async Task<List<OptionItem>> FillDataAsync(string moduleName, int callNo, CancellationToken cancellationToken)
    {
        var output = new List<OptionItem>();

        await using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken);

        const string query = "SELECT Query_String FROM adhoc_query WHERE Module_Name = @moduleName AND Call_No = @callNo";
        await using var cmd = new SqlCommand(query, conn);
        cmd.Parameters.AddWithValue("@moduleName", moduleName);
        cmd.Parameters.AddWithValue("@callNo", callNo);

        var sql = (await cmd.ExecuteScalarAsync(cancellationToken))?.ToString();
        if (string.IsNullOrWhiteSpace(sql))
        {
            return output;
        }

        using var da = new SqlDataAdapter(sql, conn);
        var dt = new DataTable();
        da.Fill(dt);

        if (dt.Rows.Count == 0 || dt.Columns.Count < 2)
        {
            return output;
        }

        for (var i = 0; i < dt.Rows.Count; i++)
        {
            var value = dt.Rows[i][0]?.ToString() ?? string.Empty;
            var text = dt.Rows[i][1]?.ToString() ?? value;
            if (!string.IsNullOrWhiteSpace(value))
            {
                output.Add(new OptionItem(value, text));
            }
        }

        return output;
    }

    private async Task<InvoiceRunResult> GenerateCoolerOnlyInvoicesAsync(
        int year,
        int month,
        Action<string>? log,
        CancellationToken cancellationToken)
    {
        var result = new InvoiceRunResult();

        const string sql = @"
SELECT DISTINCT tbrca.CustomerCode, tbrca.BranchCode
FROM tbl_Rent_Coolers_Amount tbrca
INNER JOIN tbl_Branch tbb
    ON tbrca.BusinessUnit = tbb.BusinessUnit
    AND tbrca.CustomerCode = tbb.CustomerCode
    AND tbrca.BranchCode = tbb.BranchCode
LEFT JOIN (
    SELECT CustomerCode, BranchCode
    FROM tbl_Delivering_Goods
    WHERE YEAR(VisitDate) = @year AND MONTH(VisitDate) = @month
    GROUP BY CustomerCode, BranchCode
) AS thismonthdelivery
    ON tbrca.CustomerCode = thismonthdelivery.CustomerCode
    AND tbrca.BranchCode = thismonthdelivery.BranchCode
WHERE tbrca.ToDate >= @todate
  AND thismonthdelivery.CustomerCode IS NULL";

        await using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken);
        await using var cmd = new SqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("@year", year);
        cmd.Parameters.AddWithValue("@month", month);
        cmd.Parameters.AddWithValue("@todate", _preparedDate);

        var seenCoolers = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            cancellationToken.ThrowIfCancellationRequested();

            var custCode = reader.IsDBNull(0) ? string.Empty : reader.GetString(0).Trim();
            var branchCode = reader.IsDBNull(1) ? string.Empty : reader.GetString(1).Trim();

            if (string.IsNullOrWhiteSpace(custCode) || string.IsNullOrWhiteSpace(branchCode))
            {
                continue;
            }

            var coolerKey = $"{custCode}|{branchCode}";
            if (!seenCoolers.Add(coolerKey))
            {
                continue;
            }

            result.Processed++;

            if (await InvoiceAlreadyExistsAsync(custCode, branchCode, month, year, cancellationToken))
            {
                result.Skipped++;
                log?.Invoke($"Skipped Cooler: {custCode}-{branchCode}");
                continue;
            }

            var inserted = await Task.Run(() => GenInvoice(custCode, branchCode, month, year), cancellationToken);
            if (inserted)
            {
                result.Inserted++;
                log?.Invoke($"Done Cooler: {custCode}-{branchCode}");
            }
            else
            {
                result.Skipped++;
                log?.Invoke($"Skipped Cooler (No amount/error): {custCode}-{branchCode}");
            }
        }

        return result;
    }

    private async Task<InvoiceRunResult> GenerateExecutiveWiseInvoicesAsync(
        int year,
        int month,
        Action<string>? log,
        CancellationToken cancellationToken)
    {
        var total = new InvoiceRunResult();

        const string sql = @"
SELECT DISTINCT
    y.ExecutiveCode,
    y.ExecutiveName,
    CASE WHEN y.ExecutiveCode = 'EX1540' THEN 1 ELSE 0 END AS ExecSortOrder
FROM
    (SELECT CustomerCode, BranchCode
     FROM tbl_Delivering_Goods
     WHERE YEAR(VisitDate) = @year
       AND MONTH(VisitDate) = @month) AS x
LEFT JOIN
    (SELECT CustomerCode, BranchCode, ExecutiveCode, ExecutiveName
     FROM tbl_Branch
     INNER JOIN tbl_Executive ON tbl_Branch.ExectivePerName = tbl_Executive.ExecutiveCode) AS y
ON x.CustomerCode = y.CustomerCode AND x.BranchCode = y.BranchCode
WHERE y.ExecutiveCode IS NOT NULL
ORDER BY ExecSortOrder, y.ExecutiveName";

        await using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken);
        await using var cmd = new SqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("@year", year);
        cmd.Parameters.AddWithValue("@month", month);

        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            cancellationToken.ThrowIfCancellationRequested();

            var executiveCode = reader.IsDBNull(0) ? string.Empty : reader.GetString(0);
            var executiveName = reader.IsDBNull(1) ? "Unknown Executive" : reader.GetString(1);

            log?.Invoke($"Starting Executive: {executiveName}");
            var execResult = await GenerateInvoicesForExecutiveAsync(year, month, executiveCode, log, cancellationToken);
            Merge(total, execResult);
            log?.Invoke($"Done Executive: {executiveName} (Inserted: {execResult.Inserted})");
        }

        return total;
    }

    private async Task<InvoiceRunResult> GenerateInvoicesForExecutiveAsync(
        int year,
        int month,
        string executiveCode,
        Action<string>? log,
        CancellationToken cancellationToken)
    {
        var result = new InvoiceRunResult();

        const string sql = @"
SELECT tbdg.CustomerCode, tbdg.BranchCode
FROM tbl_Delivering_Goods tbdg
LEFT OUTER JOIN tbl_Branch tbb
    ON tbdg.BusinessUnit = tbb.BusinessUnit
    AND tbdg.CustomerCode = tbb.CustomerCode
    AND tbdg.BranchCode = tbb.BranchCode
WHERE YEAR(tbdg.VisitDate) = @year
  AND MONTH(tbdg.VisitDate) = @month
  AND tbb.ExectivePerName = @executive
GROUP BY tbdg.CustomerCode, tbdg.BranchCode
ORDER BY tbdg.CustomerCode, tbdg.BranchCode";

        await using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken);
        await using var cmd = new SqlCommand(sql, conn);
        cmd.CommandTimeout = 300;
        cmd.Parameters.AddWithValue("@year", year);
        cmd.Parameters.AddWithValue("@month", month);
        cmd.Parameters.AddWithValue("@executive", executiveCode);

        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            cancellationToken.ThrowIfCancellationRequested();

            var custCode = reader.IsDBNull(0) ? string.Empty : reader.GetString(0);
            var branchCode = reader.IsDBNull(1) ? string.Empty : reader.GetString(1);
            result.Processed++;

            if (await InvoiceAlreadyExistsAsync(custCode, branchCode, month, year, cancellationToken))
            {
                result.Skipped++;
                continue;
            }

            var inserted = await Task.Run(() => GenInvoice(custCode, branchCode, month, year), cancellationToken);
            if (inserted)
            {
                result.Inserted++;
            }
            else
            {
                result.Skipped++;
                log?.Invoke($"Skipped Executive (No amount/error): {custCode}-{branchCode}");
            }
        }

        return result;
    }

    private async Task<bool> InvoiceAlreadyExistsAsync(string custcode, string brancode, int month, int year, CancellationToken cancellationToken)
    {
        const string query = @"
SELECT 1
FROM tbl_Invoice
WHERE CustomerCode = @custcode
  AND BranchCode = @brancode
  AND InMonth = @month
  AND InYear = @year";

        await using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken);
        await using var cmd = new SqlCommand(query, conn);
        cmd.Parameters.AddWithValue("@custcode", custcode);
        cmd.Parameters.AddWithValue("@brancode", brancode);
        cmd.Parameters.AddWithValue("@month", month);
        cmd.Parameters.AddWithValue("@year", year);

        var exists = await cmd.ExecuteScalarAsync(cancellationToken);
        return exists != null;
    }

    private bool GenInvoice(string custcode, string brancode, int month, int year)
    {
        try
        {
            _svat = checkSVATflag(custcode, brancode);

            var invonum = GeneratingInvoiceNumber(month, year.ToString());
            var routecode = getcustroutecode(custcode, brancode);
            getcusttaxtype(custcode, brancode, ref _taxType);
            getcoolerdetails(custcode, brancode, ref _numOfCoolers, ref _amountPerCooler);
            var collexename = getcollexename(custcode, brancode);
            var totalbottamt = gettotalnumofdispatchbottles(custcode, brancode, month, year);

            var totalamount = gettotalAoumt(custcode, brancode, month, year, totalbottamt, ref _bottlePrice);

            getcustomercoolerrentamount(custcode, brancode, ref _totalRentAmount, ref _taxOfRentAmount);

            var discountamt = caldiscountamt(custcode, brancode, totalamount, _totalRentAmount);
            var ndttax = CalNDT(custcode, brancode, month, year, totalamount, discountamt);
            var totaltaxamount = caltaxfortotalamount(custcode, brancode, month, year, totalamount, ndttax, discountamt);
            var delticnumber = getdelticketnumbers(custcode, brancode, month, year);

            var tottaxamt = totaltaxamount;
            var totalamtpercust = ((totalamount + _totalRentAmount) - discountamt) + ndttax + tottaxamt;
            var grandTotal = totalamtpercust;

            if (totalbottamt <= 0 && _totalRentAmount <= 0)
            {
                ResetRuntimeValues();
                return false;
            }

            using var conn = new SqlConnection(_connectionString);
            conn.Open();

            const string insertQuery = @"
INSERT INTO tbl_Invoice (
    BusinessUnit, InvoiceNum, CustomerCode, BranchCode, RouteCode, PricePack,
    InYear, InMonth, TotalconsumBotts, BottlePrice, Amount, TaxAmount, DiscountAmount,
    TotalAmount, DateofPrepared, CoolersRentAmount, GrandTotal, InvoicesCancel,
    InvoicesSetoff, Taxamtofcoreamt, PaidAmount, DelTicNum, CollectionExective,
    NumofCoolers, Amtpercooler, NDT
)
VALUES (
    @0, @1, @2, @3, @4, @5,
    @6, @7, @8, @9, @10, @11, @12,
    @13, @14, @15, @16, @17, @18,
    @19, @20, @21, @22, @23, @24, @25
)";

            using var cmd = new SqlCommand(insertQuery, conn);
            cmd.Parameters.AddWithValue("@0", "APWT");
            cmd.Parameters.AddWithValue("@1", invonum);
            cmd.Parameters.AddWithValue("@2", custcode);
            cmd.Parameters.AddWithValue("@3", brancode);
            cmd.Parameters.AddWithValue("@4", routecode);
            cmd.Parameters.AddWithValue("@5", _pricePackage);
            cmd.Parameters.AddWithValue("@6", year);
            cmd.Parameters.AddWithValue("@7", month);
            cmd.Parameters.AddWithValue("@8", totalbottamt);
            cmd.Parameters.AddWithValue("@9", Math.Round(_bottlePrice, 2));
            cmd.Parameters.AddWithValue("@10", Math.Round(totalamount, 2));
            cmd.Parameters.AddWithValue("@11", _taxType == "V1" ? 0 : Math.Round(tottaxamt, 2));
            cmd.Parameters.AddWithValue("@12", Math.Round(discountamt, 2));
            cmd.Parameters.AddWithValue("@13", Math.Round(totalamtpercust, 2));
            cmd.Parameters.AddWithValue("@14", _preparedDate);
            cmd.Parameters.AddWithValue("@15", Math.Round(_totalRentAmount, 2));
            cmd.Parameters.AddWithValue("@16", Math.Round(grandTotal, 2));
            cmd.Parameters.AddWithValue("@17", 0);
            cmd.Parameters.AddWithValue("@18", 0);
            cmd.Parameters.AddWithValue("@19", 0);
            cmd.Parameters.AddWithValue("@20", 0);
            cmd.Parameters.AddWithValue("@21", delticnumber);
            cmd.Parameters.AddWithValue("@22", collexename);
            cmd.Parameters.AddWithValue("@23", _numOfCoolers);
            cmd.Parameters.AddWithValue("@24", Math.Round(_amountPerCooler, 2));
            cmd.Parameters.AddWithValue("@25", Math.Round(ndttax, 2));

            var rowsAffected = cmd.ExecuteNonQuery();
            if (rowsAffected > 0)
            {
                InsertValuesToInvoiceDetails(invonum, custcode, brancode, month, year, _preparedDate);
                InsertDataToMonthlyInvoiceTable(custcode, brancode, month, year);
            }

            ResetRuntimeValues();
            return rowsAffected > 0;
        }
        catch
        {
            ResetRuntimeValues();
            return false;
        }
    }

    private void ResetRuntimeValues()
    {
        _totalRentAmount = 0;
        _taxOfRentAmount = 0;
        _pricePackage = string.Empty;
        _paymentType = string.Empty;
        _discount = string.Empty;
        _taxType = string.Empty;
        _bottlePrice = 0;
        _numOfCoolers = 0;
        _amountPerCooler = 0;
        _svat = false;
    }

    private bool checkSVATflag(string custcode, string brancode)
    {
        const string query = @"
SELECT SVATflag
FROM tbl_Branch
WHERE CustomerCode = @custcode AND BranchCode = @brancode";

        using var conn = new SqlConnection(_connectionString);
        conn.Open();
        using var cmd = new SqlCommand(query, conn);
        cmd.Parameters.AddWithValue("@custcode", custcode);
        cmd.Parameters.AddWithValue("@brancode", brancode);

        var value = cmd.ExecuteScalar();
        if (value is null || value == DBNull.Value)
        {
            return false;
        }

        return Convert.ToBoolean(value);
    }

    private string GeneratingInvoiceNumber(int month, string year)
    {
        var year1 = year.Substring(2, 2);
        var yearmonth = _svat ? $"S{year1}{month}" : $"{year1}{month}";
        var invoicenum = string.Empty;

        const string adhocQuerySql = @"
SELECT Query_String
FROM adhoc_query
WHERE Module_Name = 'InvoiceNumber' AND Call_No = 1";

        using var conn = new SqlConnection(_connectionString);
        conn.Open();

        string? dynamicSql;
        using (var lookupCmd = new SqlCommand(adhocQuerySql, conn))
        {
            var result = lookupCmd.ExecuteScalar();
            dynamicSql = result == null || result == DBNull.Value ? null : result.ToString();
        }

        if (string.IsNullOrWhiteSpace(dynamicSql))
        {
            return yearmonth + "000001";
        }

        using var execCmd = new SqlCommand(dynamicSql, conn);
        execCmd.CommandType = CommandType.Text;
        execCmd.Parameters.AddWithValue("@0", yearmonth);
        execCmd.Parameters.AddWithValue("@1", year);
        execCmd.Parameters.AddWithValue("@2", month);

        var scalar = execCmd.ExecuteScalar();
        if (scalar is null || scalar == DBNull.Value)
        {
            return yearmonth + "000001";
        }

        var strnum = scalar.ToString() ?? "1";
        var padded = new string('0', Math.Max(0, 6 - strnum.Length)) + strnum;
        invoicenum = yearmonth + padded;

        return invoicenum;
    }

    private string getcustroutecode(string custcode, string brancode)
    {
        const string query = @"
SELECT tbl_Executive.ExecutiveName
FROM tbl_Branch
INNER JOIN tbl_Executive
    ON tbl_Branch.BusinessUnit = tbl_Executive.BusinessUnit
    AND tbl_Branch.ExectivePerName = tbl_Executive.ExecutiveCode
WHERE tbl_Branch.CustomerCode = @custcode
AND tbl_Branch.BranchCode = @brancode";

        using var conn = new SqlConnection(_connectionString);
        conn.Open();
        using var cmd = new SqlCommand(query, conn);
        cmd.Parameters.AddWithValue("@custcode", custcode);
        cmd.Parameters.AddWithValue("@brancode", brancode);

        using var reader = cmd.ExecuteReader();
        if (reader.Read() && reader[0] != DBNull.Value)
        {
            return reader[0].ToString() ?? "None";
        }

        return "None";
    }

    private void getcusttaxtype(string custcode, string brancode, ref string taxtype)
    {
        using var conn = new SqlConnection(_connectionString);
        conn.Open();

        const string vatTypeQuery = @"
SELECT VatType
FROM tbl_Custrulebase
WHERE CustomerCode = @custcode AND BranchCode = @brancode";

        using (var cmd1 = new SqlCommand(vatTypeQuery, conn))
        {
            cmd1.Parameters.AddWithValue("@custcode", custcode);
            cmd1.Parameters.AddWithValue("@brancode", brancode);
            var result = cmd1.ExecuteScalar();
            if (result != null && result != DBNull.Value)
            {
                taxtype = result.ToString() ?? string.Empty;
                return;
            }
        }

        const string adhocQueryLookup = @"
SELECT Query_String
FROM adhoc_query
WHERE Module_Name = 'pricepackageandtaxtype' AND Call_No = 1";

        string? dynamicSql;
        using (var lookupCmd = new SqlCommand(adhocQueryLookup, conn))
        {
            var lookupResult = lookupCmd.ExecuteScalar();
            dynamicSql = lookupResult is null || lookupResult == DBNull.Value ? null : lookupResult.ToString();
        }

        if (string.IsNullOrWhiteSpace(dynamicSql))
        {
            return;
        }

        using var cmd2 = new SqlCommand(dynamicSql, conn);
        cmd2.Parameters.AddWithValue("@0", custcode);
        var scalar = cmd2.ExecuteScalar();
        if (scalar != null && scalar != DBNull.Value)
        {
            taxtype = scalar.ToString() ?? taxtype;
        }
    }

    private void getcoolerdetails(string custcode, string brancode, ref int numofcoolers, ref decimal amtpercooler)
    {
        decimal amtpcoo;
        decimal amtpcosscl;
        decimal nbtrate = 0;

        const string queryCoolers = @"
SELECT COUNT(SerialNumber) AS numofcoolers, SUM(RentAmount) AS rentamt
FROM tbl_Rent_Coolers_Amount
WHERE CustomerCode = @custcode AND BranchCode = @brancode
      AND ToDate >= @todate AND AuConfirm = @confirm";

        using var conn = new SqlConnection(_connectionString);
        conn.Open();
        using var cmd = new SqlCommand(queryCoolers, conn);
        cmd.Parameters.AddWithValue("@custcode", custcode);
        cmd.Parameters.AddWithValue("@brancode", brancode);
        cmd.Parameters.AddWithValue("@todate", _preparedDate);
        cmd.Parameters.AddWithValue("@confirm", 1);

        using var reader = cmd.ExecuteReader();
        if (!reader.Read() || reader.IsDBNull(0) || reader.IsDBNull(1))
        {
            numofcoolers = 0;
            amtpercooler = 0;
            return;
        }

        numofcoolers = Convert.ToInt32(reader[0]);
        var totalRent = Convert.ToDecimal(reader[1]);

        if (numofcoolers <= 0)
        {
            numofcoolers = 0;
            amtpercooler = 0;
            return;
        }

        if (_taxType == "V1")
        {
            reader.Close();
            const string taxQuery = "SELECT TaxRate FROM tbl_Taxs WHERE TaxCode = @code";
            using var taxCmd = new SqlCommand(taxQuery, conn);
            taxCmd.Parameters.AddWithValue("@code", "SSCL");
            var taxResult = taxCmd.ExecuteScalar();
            if (taxResult != null && taxResult != DBNull.Value)
            {
                nbtrate = Convert.ToDecimal(taxResult);
            }

            amtpcoo = totalRent / numofcoolers;
            amtpcosscl = amtpcoo + (amtpcoo * nbtrate / 100);
            var taxrat = gettaxrate(_taxType);
            amtpercooler = amtpcosscl + (amtpcosscl * taxrat / 100);
        }
        else
        {
            amtpercooler = totalRent / numofcoolers;
        }
    }

    private decimal gettaxrate(string taxty)
    {
        const string query = @"
SELECT TaxRate
FROM tbl_Taxs
WHERE TaxCode = @taxty";

        using var conn = new SqlConnection(_connectionString);
        conn.Open();
        using var cmd = new SqlCommand(query, conn);
        cmd.Parameters.AddWithValue("@taxty", taxty);
        var result = cmd.ExecuteScalar();

        return result is null || result == DBNull.Value ? 0 : Convert.ToDecimal(result);
    }

    private string getcollexename(string custcode, string brancode)
    {
        const string query = @"
SELECT ExectivePerName
FROM tbl_Branch
WHERE CustomerCode = @custcode AND BranchCode = @brancode";

        using var conn = new SqlConnection(_connectionString);
        conn.Open();
        using var cmd = new SqlCommand(query, conn);
        cmd.Parameters.AddWithValue("@custcode", custcode);
        cmd.Parameters.AddWithValue("@brancode", brancode);

        using var reader = cmd.ExecuteReader();
        if (reader.Read() && !reader.IsDBNull(0))
        {
            return reader[0].ToString() ?? "None";
        }

        return "None";
    }

    private int gettotalnumofdispatchbottles(string custcode, string brancode, int month, int year)
    {
        const string adhocQuerySql = @"
SELECT Query_String
FROM adhoc_query
WHERE Module_Name = 'totalnumofdispatchbottles' AND Call_No = 1";

        using var conn = new SqlConnection(_connectionString);
        conn.Open();

        string? dynamicSql;
        using (var cmd = new SqlCommand(adhocQuerySql, conn))
        {
            var result = cmd.ExecuteScalar();
            dynamicSql = result == null || result == DBNull.Value ? null : result.ToString();
        }

        if (string.IsNullOrWhiteSpace(dynamicSql))
        {
            return 0;
        }

        using var cmd2 = new SqlCommand(dynamicSql, conn);
        cmd2.CommandType = CommandType.Text;
        cmd2.Parameters.AddWithValue("@0", custcode);
        cmd2.Parameters.AddWithValue("@1", brancode);
        cmd2.Parameters.AddWithValue("@2", year);
        cmd2.Parameters.AddWithValue("@3", month);

        var scalar = cmd2.ExecuteScalar();
        return scalar is null || scalar == DBNull.Value ? 0 : Convert.ToInt32(scalar);
    }

    private decimal gettotalAoumt(string custcode, string brancode, int month, int year, int totconsumbott, ref decimal botprice)
    {
        getcust_Pricegroup(custcode, brancode, ref _pricePackage);

        decimal ssclrate = 0;

        using var conn = new SqlConnection(_connectionString);
        conn.Open();

        const string taxQuery = "SELECT TaxRate FROM tbl_Taxs WHERE TaxCode = @code";
        using (var cmd1 = new SqlCommand(taxQuery, conn))
        {
            cmd1.Parameters.AddWithValue("@code", "SSCL");
            var result = cmd1.ExecuteScalar();
            if (result != null && result != DBNull.Value)
            {
                ssclrate = Convert.ToDecimal(result);
            }
        }

        const string priceQuery = @"
SELECT MinQty, MaxQty, Price
FROM tbl_PriceDetails
WHERE PriceGroup = @priceGroup
ORDER BY MinQty DESC";

        var tbl = new DataTable();
        using (var cmd2 = new SqlCommand(priceQuery, conn))
        {
            cmd2.Parameters.AddWithValue("@priceGroup", _pricePackage);
            using var adapter = new SqlDataAdapter(cmd2);
            adapter.Fill(tbl);
        }

        var i = 0;
        decimal tot = 0;
        var bottAmt = totconsumbott;

        while (i < tbl.Rows.Count)
        {
            var minQty = Convert.ToInt32(tbl.Rows[i]["MinQty"]);
            var maxQty = Convert.ToInt32(tbl.Rows[i]["MaxQty"]);
            var pri = Convert.ToDecimal(tbl.Rows[i]["Price"]);

            if (_taxType == "V1")
            {
                var taxrat = gettaxrate(_taxType);
                var sscl = pri + (pri * ssclrate) / 100;
                botprice = sscl + (sscl * taxrat) / 100;
            }
            else
            {
                botprice = pri;
            }

            if (bottAmt >= minQty && bottAmt <= maxQty)
            {
                if (bottAmt == minQty && bottAmt == maxQty)
                {
                    tot += bottAmt * botprice;
                }
                else
                {
                    tot += (bottAmt - minQty) * botprice;
                    bottAmt = minQty;
                }
            }
            else if (bottAmt >= maxQty)
            {
                tot += bottAmt * botprice;
            }

            i++;
        }

        return tot;
    }

    private void getcust_Pricegroup(string custcode, string brancode, ref string pricepackage)
    {
        const string adhocQuerySql = @"
SELECT Query_String
FROM adhoc_query
WHERE Module_Name = 'PricegroupPackage' AND Call_No = 1";

        using var conn = new SqlConnection(_connectionString);
        conn.Open();

        string? dynamicSql;
        using (var lookupCmd = new SqlCommand(adhocQuerySql, conn))
        {
            var result = lookupCmd.ExecuteScalar();
            dynamicSql = result == null || result == DBNull.Value ? null : result.ToString();
        }

        if (string.IsNullOrWhiteSpace(dynamicSql))
        {
            return;
        }

        using var cmd = new SqlCommand(dynamicSql, conn);
        cmd.CommandType = CommandType.Text;
        cmd.Parameters.AddWithValue("@0", custcode);
        cmd.Parameters.AddWithValue("@1", brancode);
        cmd.Parameters.AddWithValue("@2", 1);

        var scalar = cmd.ExecuteScalar();
        if (scalar != null && scalar != DBNull.Value)
        {
            pricepackage = scalar.ToString() ?? pricepackage;
        }
    }

    private void getcustomercoolerrentamount(string custcode, string brancode, ref decimal totalrentamt, ref decimal taxofcorent)
    {
        decimal amtpcoo;
        decimal amtpcosscl;

        using var conn = new SqlConnection(_connectionString);
        conn.Open();

        const string todateQuerySql = @"
SELECT Query_String
FROM adhoc_query
WHERE Module_Name = 'getTodatefromCoolerRentTbl' AND Call_No = 1";

        string? dynamicTodateQuery;
        using (var lookupCmd = new SqlCommand(todateQuerySql, conn))
        {
            var result = lookupCmd.ExecuteScalar();
            dynamicTodateQuery = result == null || result == DBNull.Value ? null : result.ToString();
        }

        if (string.IsNullOrWhiteSpace(dynamicTodateQuery))
        {
            totalrentamt = 0;
            taxofcorent = 0;
            return;
        }

        DateTime toDate;
        using (var cmd = new SqlCommand(dynamicTodateQuery, conn))
        {
            cmd.Parameters.AddWithValue("@0", custcode);
            cmd.Parameters.AddWithValue("@1", brancode);
            cmd.Parameters.AddWithValue("@2", _preparedDate);

            var result = cmd.ExecuteScalar();
            if (result == null || result == DBNull.Value)
            {
                totalrentamt = 0;
                taxofcorent = 0;
                return;
            }

            toDate = Convert.ToDateTime(result);
        }

        if (toDate < _preparedDate)
        {
            totalrentamt = 0;
            taxofcorent = 0;
            return;
        }

        const string paidQuerySql = @"
SELECT Query_String
FROM adhoc_query
WHERE Module_Name = 'PaidAmount' AND Call_No = 1";

        string? paidAmountQuery;
        using (var lookupPaid = new SqlCommand(paidQuerySql, conn))
        {
            var resultPaid = lookupPaid.ExecuteScalar();
            paidAmountQuery = resultPaid == null || resultPaid == DBNull.Value ? null : resultPaid.ToString();
        }

        if (string.IsNullOrWhiteSpace(paidAmountQuery))
        {
            totalrentamt = 0;
            taxofcorent = 0;
            return;
        }

        using var cmdPaid = new SqlCommand(paidAmountQuery, conn);
        cmdPaid.Parameters.AddWithValue("@0", custcode);
        cmdPaid.Parameters.AddWithValue("@1", brancode);
        cmdPaid.Parameters.AddWithValue("@2", 1);
        cmdPaid.Parameters.AddWithValue("@3", _preparedDate);

        using var adapter = new SqlDataAdapter(cmdPaid);
        var tbl1 = new DataTable();
        adapter.Fill(tbl1);

        if (tbl1.Rows.Count == 0 || tbl1.Rows[0][0] == DBNull.Value)
        {
            totalrentamt = 0;
            taxofcorent = 0;
            return;
        }

        if (_taxType == "V1")
        {
            const string ssclRateQuery = "SELECT TaxRate FROM tbl_Taxs WHERE TaxCode = @code";
            decimal nbtrate = 0;
            using (var cmdTax = new SqlCommand(ssclRateQuery, conn))
            {
                cmdTax.Parameters.AddWithValue("@code", "SSCL");
                var taxRes = cmdTax.ExecuteScalar();
                if (taxRes != null && taxRes != DBNull.Value)
                {
                    nbtrate = Convert.ToDecimal(taxRes);
                }
            }

            amtpcoo = Convert.ToDecimal(tbl1.Rows[0][0]);
            amtpcosscl = amtpcoo + (amtpcoo * nbtrate / 100);
            var taxrat = gettaxrate(_taxType);
            totalrentamt = amtpcosscl + (amtpcosscl * taxrat / 100);
            taxofcorent = 0;
        }
        else
        {
            totalrentamt = Convert.ToDecimal(tbl1.Rows[0][0]);
            taxofcorent = tbl1.Rows[0][1] == DBNull.Value ? 0 : Convert.ToDecimal(tbl1.Rows[0][1]);
        }
    }

    private decimal caldiscountamt(string custcode, string brancode, decimal totalamt, decimal coolerrentamt)
    {
        decimal discoamount = 0;

        using var conn = new SqlConnection(_connectionString);
        conn.Open();

        const string queryLookupPayType = @"
SELECT Query_String
FROM adhoc_query
WHERE Module_Name = 'paytypes_discount' AND Call_No = 1";

        string? dynamicQueryPayType;
        using (var cmdLookup = new SqlCommand(queryLookupPayType, conn))
        {
            var result = cmdLookup.ExecuteScalar();
            dynamicQueryPayType = result == null || result == DBNull.Value ? null : result.ToString();
        }

        if (string.IsNullOrWhiteSpace(dynamicQueryPayType))
        {
            return 0;
        }

        using var cmd = new SqlCommand(dynamicQueryPayType, conn);
        cmd.Parameters.AddWithValue("@0", custcode);
        cmd.Parameters.AddWithValue("@1", brancode);
        cmd.Parameters.AddWithValue("@2", 1);
        cmd.Parameters.AddWithValue("@3", "Active");

        using var adapter = new SqlDataAdapter(cmd);
        var tbl = new DataTable();
        adapter.Fill(tbl);

        if (tbl.Rows.Count == 0 || tbl.Rows[0][0] == DBNull.Value)
        {
            return 0;
        }

        _paymentType = tbl.Rows[0][0].ToString() ?? string.Empty;
        _discount = tbl.Rows[0][1].ToString() ?? string.Empty;
        var toDate = Convert.ToDateTime(tbl.Rows[0][2]);

        if (toDate < _preparedDate || _paymentType != "PT01")
        {
            return 0;
        }

        const string queryLookupDiscount = @"
SELECT Query_String
FROM adhoc_query
WHERE Module_Name = 'caldisvalue' AND Call_No = 1";

        string? dynamicQueryDiscount;
        using (var cmdLookup2 = new SqlCommand(queryLookupDiscount, conn))
        {
            var result = cmdLookup2.ExecuteScalar();
            dynamicQueryDiscount = result == null || result == DBNull.Value ? null : result.ToString();
        }

        if (string.IsNullOrWhiteSpace(dynamicQueryDiscount))
        {
            return 0;
        }

        using var cmdDisc = new SqlCommand(dynamicQueryDiscount, conn);
        cmdDisc.Parameters.AddWithValue("@0", _discount);
        var value = cmdDisc.ExecuteScalar();

        if (value != null && value != DBNull.Value)
        {
            var discovalue = Convert.ToDecimal(value);
            discoamount = (totalamt + coolerrentamt) * discovalue;
        }

        return discoamount;
    }

    private decimal CalNDT(string custcode, string brancode, int month, int year, decimal totamt, decimal dicountva)
    {
        decimal nDTamount;
        decimal nbtrate = 0;
        var ssclex = checkSSCLExclude(custcode, brancode);
        var nbtflag = false;

        using (var conn = new SqlConnection(_connectionString))
        {
            conn.Open();

            const string ssclQuery = "SELECT TaxRate FROM tbl_Taxs WHERE TaxCode = @code";
            using (var cmd = new SqlCommand(ssclQuery, conn))
            {
                cmd.Parameters.AddWithValue("@code", "SSCL");
                var result = cmd.ExecuteScalar();
                if (result != null && result != DBNull.Value)
                {
                    nbtrate = Convert.ToDecimal(result);
                }
            }

            const string nbtQuery = "SELECT NBTflag FROM tbl_Branch WHERE CustomerCode = @custcode AND BranchCode = @brancode";
            using (var cmd = new SqlCommand(nbtQuery, conn))
            {
                cmd.Parameters.AddWithValue("@custcode", custcode);
                cmd.Parameters.AddWithValue("@brancode", brancode);
                var result = cmd.ExecuteScalar();
                if (result != null && result != DBNull.Value)
                {
                    nbtflag = Convert.ToBoolean(result);
                }
            }
        }

        if (!nbtflag)
        {
            return 0;
        }

        if (_taxType == "V2" && !ssclex)
        {
            nDTamount = ((totamt + _totalRentAmount) - dicountva) * (nbtrate / 100);
        }
        else
        {
            nDTamount = 0;
        }

        return nDTamount;
    }

    private bool checkSSCLExclude(string ccode, string bcode)
    {
        const string query = @"
SELECT SSCL_Exclude
FROM tbl_Branch
WHERE CustomerCode = @custcode AND BranchCode = @brancode";

        using var conn = new SqlConnection(_connectionString);
        conn.Open();
        using var cmd = new SqlCommand(query, conn);
        cmd.Parameters.AddWithValue("@custcode", ccode);
        cmd.Parameters.AddWithValue("@brancode", bcode);

        var result = cmd.ExecuteScalar();
        return result != null && result != DBNull.Value && Convert.ToBoolean(result);
    }

    private decimal caltaxfortotalamount(string custcode, string brancode, int month, int year, decimal totamt, decimal ndtamt, decimal dicountva)
    {
        var taxrat = (_taxType == "B1" || _taxType == "V0" || _taxType == "V1" || _taxType == "V2" || _taxType == "V3")
            ? gettaxrate(_taxType)
            : 0;

        if (_taxType == "V1")
        {
            return 0;
        }

        return ((((totamt + _totalRentAmount) - dicountva) + ndtamt) * taxrat) / 100;
    }

    private string getdelticketnumbers(string ccode, string bcode, int month, int year)
    {
        var delticnums = string.Empty;

        const string query = @"
SELECT
    CASE
        WHEN Transreference IS NULL THEN LTRIM(RTRIM(DeliTickNum))
        ELSE LTRIM(RTRIM(Transreference))
    END AS DeliTickNum
FROM tbl_Delivering_Goods
WHERE CustomerCode = @ccode
  AND BranchCode = @bcode
  AND MONTH(VisitDate) = @month
  AND YEAR(VisitDate) = @year
  AND Deliveredbottamt <> 0
ORDER BY VisitDate";

        using var conn = new SqlConnection(_connectionString);
        conn.Open();
        using var cmd = new SqlCommand(query, conn);
        cmd.Parameters.AddWithValue("@ccode", ccode);
        cmd.Parameters.AddWithValue("@bcode", bcode);
        cmd.Parameters.AddWithValue("@month", month);
        cmd.Parameters.AddWithValue("@year", year);

        using var reader = cmd.ExecuteReader();
        if (!reader.HasRows)
        {
            return "None";
        }

        while (reader.Read())
        {
            if (reader[0] != DBNull.Value)
            {
                delticnums += reader[0].ToString() + ",";
            }
        }

        return delticnums;
    }

    private void InsertValuesToInvoiceDetails(string invonum, string custcode, string brancode, int inmonth, int inyear, DateTime invodate)
    {
        using var conn = new SqlConnection(_connectionString);
        conn.Open();

        using var cmd = conn.CreateCommand();
        using var tx = conn.BeginTransaction();
        cmd.Connection = conn;
        cmd.Transaction = tx;

        try
        {
            var mon = inmonth == 1 ? 12 : inmonth - 1;
            var prevYear = inmonth == 1 ? inyear - 1 : inyear;

            cmd.CommandText = @"
INSERT INTO tbl_InvoiceDetails
(BusinessUnit, InvoiceNum, CustomerCode, BranchCode, TransactionDate, TNumber, Tdescription, Amount, SerialNum)
SELECT BusinessUnit, InvoiceNum, CustomerCode, BranchCode, DateofPrepared, InvoiceNum, 'This Month Invoice', GrandTotal, 1
FROM tbl_Invoice
WHERE MONTH(DateofPrepared) = @month AND YEAR(DateofPrepared) = @year
      AND CustomerCode = @custcode AND BranchCode = @brancode";
            cmd.Parameters.Clear();
            cmd.Parameters.AddWithValue("@month", inmonth);
            cmd.Parameters.AddWithValue("@year", inyear);
            cmd.Parameters.AddWithValue("@custcode", custcode);
            cmd.Parameters.AddWithValue("@brancode", brancode);
            cmd.ExecuteNonQuery();

            cmd.CommandText = @"
INSERT INTO tbl_InvoiceDetails
(BusinessUnit, InvoiceNum, CustomerCode, BranchCode, TransactionDate, TNumber, Tdescription, Amount, SerialNum)
SELECT tb.BusinessUnit, @invonum, tb.CustomerCode, tb.BranchCode, tb.TransactionDate, tb.TransactionTypeNo, tb.Description, tb.Outstanding, 3
FROM tbl_Transactions_BranchWise tb
INNER JOIN tbl_Item_Types t ON tb.BusinessUnit = t.BusinessUnit AND tb.TransactionTypeCode = t.Item_Type
WHERE t.ConsiderforMoIn = 1 AND MONTH(tb.TransactionDate) = @month
      AND YEAR(tb.TransactionDate) = @year AND tb.Outstanding <> 0 AND tb.Credit = 0
      AND tb.CustomerCode = @custcode AND tb.BranchCode = @brancode";
            cmd.Parameters.Clear();
            cmd.Parameters.AddWithValue("@invonum", invonum);
            cmd.Parameters.AddWithValue("@month", inmonth);
            cmd.Parameters.AddWithValue("@year", inyear);
            cmd.Parameters.AddWithValue("@custcode", custcode);
            cmd.Parameters.AddWithValue("@brancode", brancode);
            cmd.ExecuteNonQuery();

            cmd.CommandText = @"
SELECT SUM(t.Outstanding)
FROM tbl_Transactions_BranchWise t
INNER JOIN tbl_Item_Types it ON t.BusinessUnit = it.BusinessUnit AND t.TransactionTypeCode = it.Item_Type
WHERE it.ConsiderforMoIn = 1 AND MONTH(t.TransactionDate) = @mon AND YEAR(t.TransactionDate) = @pyear
      AND t.CustomerCode = @custcode AND t.BranchCode = @brancode AND t.Outstanding <> 0";
            cmd.Parameters.Clear();
            cmd.Parameters.AddWithValue("@mon", mon);
            cmd.Parameters.AddWithValue("@pyear", prevYear);
            cmd.Parameters.AddWithValue("@custcode", custcode);
            cmd.Parameters.AddWithValue("@brancode", brancode);
            var lastMonthOutstd = cmd.ExecuteScalar();
            var lastOutAmt = lastMonthOutstd == DBNull.Value || lastMonthOutstd is null ? 0 : Convert.ToDecimal(lastMonthOutstd);

            if (lastOutAmt > 0)
            {
                cmd.CommandText = @"
SELECT TOP 1 TransactionTypeNo
FROM tbl_Transactions_BranchWise
WHERE CustomerCode = @custcode AND BranchCode = @brancode
      AND TransactionTypeCode = 'INV03' AND MONTH(TransactionDate) = @mon
      AND YEAR(TransactionDate) = @pyear AND Outstanding <> 0";
                cmd.Parameters.Clear();
                cmd.Parameters.AddWithValue("@custcode", custcode);
                cmd.Parameters.AddWithValue("@brancode", brancode);
                cmd.Parameters.AddWithValue("@mon", mon);
                cmd.Parameters.AddWithValue("@pyear", prevYear);
                var tNum = cmd.ExecuteScalar();

                cmd.CommandText = @"
INSERT INTO tbl_InvoiceDetails
(BusinessUnit, InvoiceNum, CustomerCode, BranchCode, TransactionDate, TNumber, Tdescription, Amount, SerialNum)
VALUES ('APWT', @invonum, @custcode, @brancode, @invodate, @transNum, 'Last Month Outstanding', @amount, 4)";
                cmd.Parameters.Clear();
                cmd.Parameters.AddWithValue("@invonum", invonum);
                cmd.Parameters.AddWithValue("@custcode", custcode);
                cmd.Parameters.AddWithValue("@brancode", brancode);
                cmd.Parameters.AddWithValue("@invodate", invodate);
                cmd.Parameters.AddWithValue("@transNum", tNum ?? "");
                cmd.Parameters.AddWithValue("@amount", lastOutAmt);
                cmd.ExecuteNonQuery();
            }

            var over60 = invodate.AddMonths(-2);

            cmd.CommandText = @"
SELECT SUM(b.Outstanding)
FROM (
    SELECT tb.Outstanding
    FROM tbl_Transactions_BranchWise tb
    INNER JOIN tbl_Item_Types t ON tb.TransactionTypeCode = t.Item_Type AND tb.BusinessUnit = t.BusinessUnit
    WHERE tb.CustomerCode = @custcode AND tb.BranchCode = @brancode
          AND tb.TransactionDate <= @over60 AND tb.Outstanding <> 0 AND t.ConsiderforMoIn = 1 AND tb.Credit = 0
    UNION ALL
    SELECT ta.Outstanding
    FROM tbl_Transaction_Archive ta
    INNER JOIN tbl_Item_Types t ON ta.TransactionTypeCode = t.Item_Type AND ta.BusinessUnit = t.BusinessUnit
    WHERE ta.CustomerCode = @custcode AND ta.BranchCode = @brancode
          AND ta.TransactionDate <= @over60 AND ta.Outstanding <> 0 AND t.ConsiderforMoIn = 1 AND ta.Credit = 0
) AS b";
            cmd.Parameters.Clear();
            cmd.Parameters.AddWithValue("@custcode", custcode);
            cmd.Parameters.AddWithValue("@brancode", brancode);
            cmd.Parameters.AddWithValue("@over60", over60);
            var over60AmtObj = cmd.ExecuteScalar();
            var over60Amt = over60AmtObj == DBNull.Value || over60AmtObj is null ? 0 : Convert.ToDecimal(over60AmtObj);

            if (over60Amt > 0)
            {
                cmd.CommandText = @"
INSERT INTO tbl_InvoiceDetails
(BusinessUnit, InvoiceNum, CustomerCode, BranchCode, TransactionDate, TNumber, Tdescription, Amount, SerialNum)
VALUES ('APWT', @invonum, @custcode, @brancode, @invodate, '', 'Over 60 days', @amount, 5)";
                cmd.Parameters.Clear();
                cmd.Parameters.AddWithValue("@invonum", invonum);
                cmd.Parameters.AddWithValue("@custcode", custcode);
                cmd.Parameters.AddWithValue("@brancode", brancode);
                cmd.Parameters.AddWithValue("@invodate", invodate);
                cmd.Parameters.AddWithValue("@amount", over60Amt);
                cmd.ExecuteNonQuery();
            }

            cmd.CommandText = "SELECT dbo.getCreditBalance('APWT', @custcode, @brancode, @invodate)";
            cmd.Parameters.Clear();
            cmd.Parameters.AddWithValue("@custcode", custcode);
            cmd.Parameters.AddWithValue("@brancode", brancode);
            cmd.Parameters.AddWithValue("@invodate", invodate);
            var creditObj = cmd.ExecuteScalar();
            var credit = creditObj == DBNull.Value || creditObj is null ? 0 : Convert.ToDecimal(creditObj);

            if (credit > 0)
            {
                cmd.CommandText = @"
INSERT INTO tbl_InvoiceDetails
(BusinessUnit, InvoiceNum, CustomerCode, BranchCode, TransactionDate, TNumber, Tdescription, Amount, SerialNum)
VALUES ('APWT', @invonum, @custcode, @brancode, @invodate, '', 'Credit Balance', @amount, 6)";
                cmd.Parameters.Clear();
                cmd.Parameters.AddWithValue("@invonum", invonum);
                cmd.Parameters.AddWithValue("@custcode", custcode);
                cmd.Parameters.AddWithValue("@brancode", brancode);
                cmd.Parameters.AddWithValue("@invodate", invodate);
                cmd.Parameters.AddWithValue("@amount", -1 * credit);
                cmd.ExecuteNonQuery();
            }

            tx.Commit();
        }
        catch
        {
            tx.Rollback();
            throw;
        }
    }

    private void InsertDataToMonthlyInvoiceTable(string custcode, string brancode, int inmonth, int inyear)
    {
        using var conn = new SqlConnection(_connectionString);
        conn.Open();

        string invotype = string.Empty;
        string vatnum = string.Empty;

        using (var cmd = new SqlCommand("SELECT invoicetype, VATNum FROM getcustvatnumber(@cust, @bran)", conn))
        {
            cmd.Parameters.AddWithValue("@cust", custcode);
            cmd.Parameters.AddWithValue("@bran", brancode);
            using var reader = cmd.ExecuteReader();
            if (reader.Read())
            {
                invotype = reader["invoicetype"].ToString() ?? string.Empty;
                vatnum = reader["VATNum"].ToString() ?? string.Empty;
            }
        }

        const string insertSql = @"
INSERT INTO MonthlyInvoice (
    BusinessUnit, InvoiceNum, CustomerCode, BranchCode, RouteCode, PricePack, InYear, InMonth,
    TotalconsumBotts, BottlePrice, Amount, TaxAmount, DiscountAmount, TotalAmount,
    CoolersRentAmount, Taxamtofcoreamt, GrandTotal, OutstandingAmt, DateofPrepared,
    DateofPrinted, BillingAddNum, BillingAddstrname, BillingAddCity, DiliveryTiketNo,
    InvoiceType, VatNum, Branchname, Telenumber, itemDate, itemNumber, itemDescription,
    itemAmount, SerialNo, CollectionExective, NumofCoolers, Amtpercooler, NDT
)
SELECT
    tbin.BusinessUnit, tbin.InvoiceNum, tbin.CustomerCode, tbin.BranchCode, tbin.RouteCode,
    dbo.get_Customer_Invoice_Delivery_Method(tbin.CustomerCode, tbin.BranchCode),
    tbin.InYear, tbin.InMonth, tbin.TotalconsumBotts, tbin.BottlePrice, tbin.Amount,
    tbin.TaxAmount, tbin.DiscountAmount, tbin.TotalAmount, tbin.CoolersRentAmount,
    tbin.Taxamtofcoreamt, tbin.GrandTotal, 0, tbin.DateofPrepared, tbin.DateofPrepared,
    tbcca.BillingAddNum, tbcca.BillingAddstrname, tbcca.BillingAddCity, tbin.DelTicNum,
    @invotype, @vatnum, tbb.BranchName, tbcca.Bill_Mobile_Num1,
    CAST(tbinde.TransactionDate AS DATE),
    tbinde.TNumber, tbinde.Tdescription, tbinde.Amount, tbinde.SerialNum,
    tbin.CollectionExective, tbin.NumofCoolers, tbin.Amtpercooler, tbin.NDT
FROM tbl_Invoice tbin
INNER JOIN tbl_InvoiceDetails tbinde ON
    tbin.BusinessUnit = tbinde.BusinessUnit AND
    tbin.InvoiceNum = tbinde.InvoiceNum AND
    tbin.CustomerCode = tbinde.CustomerCode AND
    tbin.BranchCode = tbinde.BranchCode
INNER JOIN tbl_Branch tbb ON
    tbin.BusinessUnit = tbb.BusinessUnit AND
    tbin.CustomerCode = tbb.CustomerCode AND
    tbin.BranchCode = tbb.BranchCode
INNER JOIN tbl_Current_Cust_Addresses tbcca ON
    tbb.BusinessUnit = tbcca.BusinessUnit AND
    tbb.CustomerCode = tbcca.CustomerCode AND
    tbb.BranchCode = tbcca.BranchCode
WHERE tbin.InYear = @inyear
  AND tbin.InMonth = @inmonth
  AND tbin.CustomerCode = @custcode
  AND tbin.BranchCode = @brancode
  AND tbcca.Latest = 1;";

        using var insert = new SqlCommand(insertSql, conn);
        insert.Parameters.AddWithValue("@inyear", inyear);
        insert.Parameters.AddWithValue("@inmonth", inmonth);
        insert.Parameters.AddWithValue("@custcode", custcode);
        insert.Parameters.AddWithValue("@brancode", brancode);
        insert.Parameters.AddWithValue("@invotype", invotype);
        insert.Parameters.AddWithValue("@vatnum", vatnum);
        insert.ExecuteNonQuery();
    }
}
