using APWS.Dtos.MonthlyInvoice;
using APWS.Models;
using APWS.Models.MonthlyInvoice;
using APWS.Services;
using Microsoft.AspNetCore.Mvc;

namespace APWS.Controllers;

[ApiController]
[Route("api/monthly-invoice")]
public sealed class MonthlyInvoiceController : ControllerBase
{
    private readonly MonthlyInvoiceService _invoiceService;

    public MonthlyInvoiceController(MonthlyInvoiceService invoiceService)
    {
        _invoiceService = invoiceService;
    }

    [HttpGet("months")]
    public async Task<ActionResult<IReadOnlyList<OptionItemDto>>> GetMonthsAsync(CancellationToken cancellationToken)
    {
        var options = await _invoiceService.GetMonthsAsync(cancellationToken);
        return Ok(options.Select(Map).ToList());
    }

    [HttpGet("executives")]
    public async Task<ActionResult<IReadOnlyList<OptionItemDto>>> GetExecutivesAsync(CancellationToken cancellationToken)
    {
        var options = await _invoiceService.GetExecutivesAsync(cancellationToken);
        return Ok(options.Select(Map).ToList());
    }

    [HttpGet("branches/{customerCode}")]
    public async Task<ActionResult<IReadOnlyList<OptionItemDto>>> GetBranchesAsync(string customerCode, CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(customerCode))
        {
            return BadRequest("Customer code is required.");
        }

        var options = await _invoiceService.GetBranchesAsync(customerCode, cancellationToken);
        return Ok(options.Select(Map).ToList());
    }

    [HttpPost("generate/all")]
    public async Task<ActionResult<InvoiceGenerationResponseDto>> GenerateAllAsync(
        [FromBody] GenerateAllMonthlyInvoiceRequestDto request,
        CancellationToken cancellationToken)
    {
        var model = new GenerateAllMonthlyInvoiceModel
        {
            Year = request.Year,
            Month = request.Month,
            PreparedDate = request.PreparedDate,
            CoolerOnly = request.CoolerOnly
        };

        var logs = new List<string>();
        var result = await _invoiceService.GenerateAllAsync(model, logs.Add, cancellationToken);

        return Ok(new InvoiceGenerationResponseDto
        {
            Result = Map(result),
            Logs = logs
        });
    }

    [HttpPost("generate/single")]
    public async Task<ActionResult<InvoiceGenerationResponseDto>> GenerateSingleAsync(
        [FromBody] GenerateSingleMonthlyInvoiceRequestDto request,
        CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(request.CustomerCode) || string.IsNullOrWhiteSpace(request.BranchCode))
        {
            return BadRequest("Customer code and branch code are required.");
        }

        var model = new GenerateSingleMonthlyInvoiceModel
        {
            CustomerCode = request.CustomerCode.Trim(),
            BranchCode = request.BranchCode.Trim(),
            Year = request.Year,
            Month = request.Month,
            PreparedDate = request.PreparedDate
        };

        var logs = new List<string>();
        var result = await _invoiceService.GenerateSingleAsync(model, logs.Add, cancellationToken);

        return Ok(new InvoiceGenerationResponseDto
        {
            Result = Map(result),
            Logs = logs
        });
    }

    private static OptionItemDto Map(OptionItem item)
        => new(item.Value, item.Text);

    private static InvoiceRunResultDto Map(InvoiceRunResult result)
        => new()
        {
            Processed = result.Processed,
            Inserted = result.Inserted,
            Skipped = result.Skipped
        };
}
