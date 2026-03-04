import numpy as np
import matplotlib.pyplot as plt

def sigmoid(x):
    """标准Sigmoid函数"""
    return 1 / (1 + np.exp(-x))

def sigmoid_param(x, k=1, x0=0):
    """带参数的Sigmoid函数"""
    return 1 / (1 + np.exp(-k * (x - x0)))

# 测试
x_values = np.linspace(-10, 10, 400)
y_values = sigmoid(x_values)

print("Sigmoid函数值示例:")
for x in [-5, -2, 0, 2, 5]:
    print(f"σ({x:2.0f}) = {sigmoid(x):.6f}")

# 可视化
plt.figure(figsize=(10, 6))

# 标准Sigmoid
plt.plot(x_values, y_values, 'b-', linewidth=3, label='标准 Sigmoid: σ(x)=1/(1+e^(-x))')

# 不同参数的Sigmoid
for k in [0.5, 2, 5]:
    y_param = sigmoid_param(x_values, k=k)
    plt.plot(x_values, y_param, '--', linewidth=2, label=f'k={k}')

plt.axhline(y=0.5, color='gray', linestyle=':', alpha=0.5)
plt.axvline(x=0, color='gray', linestyle=':', alpha=0.5)
plt.xlabel('x', fontsize=12)
plt.ylabel('σ(x)', fontsize=12)
plt.title('Sigmoid函数及其参数变化', fontsize=14)
plt.grid(True, alpha=0.3)
plt.legend()
plt.ylim(-0.1, 1.1)
plt.show()