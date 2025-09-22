from typing import Any, Final, List, Tuple

import numpy as np
import pandas as pd
import torch
import torch.nn as nn
import torch.optim as optim


def process(
    data: pd.DataFrame,
    method: str,
) -> Tuple[pd.DataFrame, int]:
    items = data["item_id"].unique()
    alpha, beta = 1.0, 0.0
    failed = 0
    try:
        match method:
            case "full":
                pass
            case "transactions":
                good_indices = np.where(
                    np.logical_and(
                        data["promo_quantity"].notnull(),
                        data["sales_quantity"].notnull(),
                    )
                )[0]
                if good_indices.shape[0] >= 2:
                    data = data.iloc[good_indices]
                    alpha, beta = linear_regression(
                        data["theoretical_discount"].to_numpy(),
                        data["promo_quantity"].to_numpy()
                        / data["sales_quantity"].to_numpy(),
                    )
            case "discount":
                good_indices = np.where(
                    np.logical_and(
                        np.logical_and(
                            data["theoretical_discount"].notnull(),
                            data["effective_discount"].notnull(),
                        ),
                        data["theoretical_discount"] > 0,
                    )
                )[0]
                if good_indices.shape[0] >= 2:
                    data = data.iloc[good_indices]
                    """
                    sales_revenue = promo_quantity * regular_price * (1-theoretical_discount) + (sales_quantity - promo_quantity) * regular_price
                    ->
                    promo_quantity = ( sales_quantity * regular_price - sales_revenue ) / ( regular_price * theoretical_discount )
                    Since:
                        sales_revenue = sales_quantity * regular_price - sales_quantity * regular_price * (1 - effective_discount)
                    ->
                    promo_quantity = ( sales_quantity * regular_price - sales_quantity * regular_price * (1 - effective_discount) ) / ( regular_price * theoretical_discount )
                    ->
                    promo_quantity = ( sales_quantity * effective_discount) / ( theoretical_discount )
                    """
                    promo_quantity = (
                        data["sales_quantity"] * data["effective_discount"]
                    ) / (data["theoretical_discount"])

                    alpha, beta = linear_regression(
                        data["theoretical_discount"].to_numpy(),
                        promo_quantity.to_numpy() / data["sales_quantity"].to_numpy(),
                    )

    except Exception as e:
        alpha = 1.0
        beta = 0.0
        failed = 1

    out = pd.DataFrame({"item_id": items, "alpha": alpha, "beta": beta})

    return out, failed




def linear_regression(x: np.ndarray, y: np.ndarray) -> Tuple[float, float]:
    """
    Constrained linear regression on original scale:
      y_hat = intercept + slope * x,
      intercept in [0,1], slope >= 0, never returns NaNs.
    Efficient for tiny problems via LBFGS; falls back safely on any error.
    """
    # Ensure 1-D float arrays; drop non-finite
    x = np.asarray(x, dtype=np.float64).ravel()
    y = np.asarray(y, dtype=np.float64).ravel()
    mask = np.isfinite(x) & np.isfinite(y)
    x, y = x[mask], y[mask]

    # Degenerate or too few points → safe default
    if x.size < 2:
        return 1.0, 0.0

    # Tensors
    X = torch.from_numpy(x).to(dtype=torch.float32)
    Y = torch.from_numpy(y).to(dtype=torch.float32)

    # Unconstrained parameters
    intercept_raw = nn.Parameter(torch.tensor(0.0))  # intercept via sigmoid
    slope_raw = nn.Parameter(torch.tensor(0.0))      # slope via softplus

    def intercept_fn():
        return torch.sigmoid(intercept_raw)          # in [0,1]
    def slope_fn():
        return torch.nn.functional.softplus(slope_raw)  # >= 0

    # Small L2 helps robustness; LBFGS converges fast on tiny problems
    optimizer = optim.LBFGS([intercept_raw, slope_raw], max_iter=60, line_search_fn="strong_wolfe")
    l2 = 1e-6

    def closure():
        optimizer.zero_grad(set_to_none=True)
        yhat = intercept_fn() + slope_fn() * X
        loss = torch.mean((yhat - Y)**2) + l2*(intercept_raw*intercept_raw + slope_raw*slope_raw)
        # Guard against NaN/Inf loss (shouldn't happen with this parametrisation)
        if not torch.isfinite(loss):
            # Return a finite, high loss to let optimizer back off
            loss = torch.tensor(1e6, dtype=Y.dtype, requires_grad=True)
        loss.backward()
        return loss

    try:
        optimizer.step(closure)
        intercept = float(torch.clamp(intercept_fn(), 0.0, 1.0).detach().cpu().numpy())
        slope = float(torch.clamp(slope_fn(), min=0.0).detach().cpu().numpy())
        # Final safety: never return NaN
        if not np.isfinite(intercept) or not np.isfinite(slope):
            return 1.0, 0.0
        return intercept, slope
    except Exception:
        # Absolute fallback, per spec
        return 1.0, 0.0


