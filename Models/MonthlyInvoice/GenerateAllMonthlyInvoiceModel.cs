namespace APWS.Models.MonthlyInvoice;

public sealed class GenerateAllMonthlyInvoiceModel
{
    public int Year { get; init; }
    public int Month { get; init; }
    public DateTime PreparedDate { get; init; }
    public bool CoolerOnly { get; init; }
}
