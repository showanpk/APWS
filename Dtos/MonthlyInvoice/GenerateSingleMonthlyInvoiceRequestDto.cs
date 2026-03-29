using System.ComponentModel.DataAnnotations;

namespace APWS.Dtos.MonthlyInvoice;

public sealed class GenerateSingleMonthlyInvoiceRequestDto
{
    [Required]
    public string CustomerCode { get; set; } = string.Empty;

    [Required]
    public string BranchCode { get; set; } = string.Empty;

    [Range(2000, 3000)]
    public int Year { get; set; }

    [Range(1, 12)]
    public int Month { get; set; }

    public DateTime PreparedDate { get; set; }
}
