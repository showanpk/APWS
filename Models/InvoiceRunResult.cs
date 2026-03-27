namespace APWS.Models;

public sealed class InvoiceRunResult
{
    public int Processed { get; set; }
    public int Inserted { get; set; }
    public int Skipped { get; set; }
}
