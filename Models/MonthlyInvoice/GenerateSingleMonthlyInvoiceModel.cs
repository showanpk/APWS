namespace APWS.Models.MonthlyInvoice;

public sealed class GenerateSingleMonthlyInvoiceModel
{
    public string CustomerCode { get; init; } = string.Empty;
    public string BranchCode { get; init; } = string.Empty;
    public int Year { get; init; }
    public int Month { get; init; }
    public DateTime PreparedDate { get; init; }
}
