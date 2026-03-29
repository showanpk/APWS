using System.ComponentModel.DataAnnotations;

namespace APWS.Dtos.MonthlyInvoice;

public sealed class GenerateAllMonthlyInvoiceRequestDto
{
    [Range(2000, 3000)]
    public int Year { get; set; }

    [Range(1, 12)]
    public int Month { get; set; }

    public DateTime PreparedDate { get; set; }

    public bool CoolerOnly { get; set; }
}
