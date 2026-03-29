namespace APWS.Dtos.MonthlyInvoice;

public sealed class InvoiceRunResultDto
{
    public int Processed { get; set; }
    public int Inserted { get; set; }
    public int Skipped { get; set; }
}
