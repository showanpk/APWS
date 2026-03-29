namespace APWS.Dtos.MonthlyInvoice;

public sealed class InvoiceGenerationResponseDto
{
    public InvoiceRunResultDto Result { get; set; } = new();
    public List<string> Logs { get; set; } = new();
}
