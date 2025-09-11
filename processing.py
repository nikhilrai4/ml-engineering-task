from typing import Any, Final, List, Tuple

import numpy as np
import pandas as pd


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


def linear_regression(
    x: np.array,
    y: np.array,
) -> Tuple[float, float]:
    # Replace this with a pytorch implementation of linear regression that:
    # 1. Constrains the intercept to be between 0.0 and 1.0 inclusive
    # 2. Constrains the slope to be greater than or equal to 0.0
    # 3. Never returns NaNs
    return 1.0, 0.0
